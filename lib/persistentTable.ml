module H = Hashtbl
open Lwt.Infix
open Common
module U = Util
module FU = FileUtil

type level = Lwt_io.input_channel

type t =
  { directory : string;
    write_mutex : Lwt_mutex.t;
    mutable levels : level list }

type key_record =
  { key_payload : string;
    key_offset : int64;
    (* Empty fixed-length value which contains only a tag *)
    value_type : value;
    value_offset : int64;
    value_size : int32 }

let file_extension = "level"

let records_amount_length = 8

let key_length = max_key_length

let value_tag_length = 1

let value_offset_length = 8

let value_size_length = 4

let key_record_length =
  key_length + value_tag_length + value_offset_length + value_size_length

let key_offset index =
  Int64.of_int (records_amount_length + (index * key_record_length))

let write_key_payload channel payload =
  Lwt_io.write channel payload
  >>= fun () ->
  let skip = Int64.of_int (key_length - String.length payload) in
  let next_pos = Int64.add (Lwt_io.position channel) skip in
  Lwt_io.set_position channel next_pos

let value_tag = function
  | Nothing -> 0
  | Value _ -> 1

let write_records channel storage =
  let records_amount = H.length storage in
  let empty_key = Key "" in
  let keys = Array.make records_amount empty_key in
  let value_offset = ref (key_offset records_amount) in
  (let i = ref 0 in
   H.to_seq_keys storage |> Seq.iter (fun key -> keys.(!i) <- key; incr i));
  Array.sort compare keys;
  Lwt_io.BE.write_int64 channel (Int64.of_int records_amount)
  >>= fun () ->
  for%lwt i = 0 to records_amount - 1 do
    let key = keys.(i) in
    let (Key key_payload) = key in
    let value = H.find storage key in
    write_key_payload channel key_payload
    >>= fun () ->
    U.write_uint8 channel (value_tag value)
    >>= fun () ->
    Lwt_io.BE.write_int64 channel !value_offset
    >>= fun () ->
    let value_size =
      match value with
      | Nothing -> 0
      | Value v -> String.length v
    in
    (value_offset := Int64.(add !value_offset (of_int value_size)));
    Lwt_io.BE.write_int32 channel (Int32.of_int value_size)
  done
  >>= fun () ->
  for%lwt i = 0 to records_amount - 1 do
    match H.find storage keys.(i) with
    | Nothing -> Lwt.return_unit
    | Value v -> Lwt_io.write channel v
  done

let flush_memory_table persistent_table log memtable =
  Lwt_mutex.with_lock persistent_table.write_mutex (fun () ->
      let storage = ref (H.create 0) in
      let switch () =
        let s = memtable.storage in
        memtable.storage <- H.create (H.length s);
        memtable.previous_storage <- Some s;
        storage := s
      in
      PersistentLog.advance log switch
      >>= fun truncate_log ->
      Logs.info (fun m -> m "Start flushing of %i records" (H.length !storage));
      FU.next_file_name file_extension persistent_table.directory
      >>= fun (file_name, _) ->
      let temp_file_name = file_name ^ "_temp" in
      FU.open_output_file ~replace:true temp_file_name
      >>= fun (fd, channel) ->
      write_records channel !storage
      >>= fun () ->
      Lwt_io.flush channel
      >>= fun () ->
      Lwt_unix.fsync fd
      >>= fun () ->
      Lwt_io.close channel
      >>= fun () ->
      Lwt_unix.rename temp_file_name file_name
      >>= fun () ->
      Lwt_io.(open_file ~mode:input file_name)
      >|= (fun input_channel ->
            persistent_table.levels <- input_channel :: persistent_table.levels;
            memtable.previous_storage <- None)
      >>= truncate_log
      >|= fun () ->
      Logs.info (fun m -> m "End flushing. Result file: %s" file_name);
      H.length !storage)

let is_writing {write_mutex; _} = Lwt_mutex.is_locked write_mutex

let open_level_files directory =
  (* From the newest to the oldest *)
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  FU.FileNames.bindings file_names
  |> List.map snd |> List.rev
  |> Lwt_list.map_s Lwt_io.(open_file ~mode:input)

let initialize directory =
  open_level_files directory
  >|= fun levels -> {directory; levels; write_mutex = Lwt_mutex.create ()}

let read_exactly input_channel length =
  let buffer = Bytes.create length in
  Lwt_io.read_into_exactly input_channel buffer 0 length
  >|= fun () -> Bytes.unsafe_to_string buffer

let read_uint8 input_channel = Lwt_io.read_char input_channel >|= Char.code

let null_char = Char.chr 0

let trim_key key = String.split_on_char null_char key |> List.hd

let read_key_payload input_channel =
  read_exactly input_channel key_length >|= trim_key

let read_key_record ?key_payload channel =
  (match key_payload with
  | None -> read_key_payload channel
  | Some kp -> Lwt.return kp)
  >>= fun key_payload ->
  let key_offset = Int64.(sub (Lwt_io.position channel) (of_int key_length)) in
  read_uint8 channel
  >|= (function
        | 0 -> Nothing
        | 1 -> Value ""
        | n -> failwith @@ "Unknown value tag: " ^ string_of_int n)
  >>= fun value_type ->
  Lwt_io.BE.read_int64 channel
  >>= fun value_offset ->
  Lwt_io.BE.read_int32 channel
  >>= fun value_size ->
  Lwt.return {key_payload; value_type; key_offset; value_offset; value_size}

let rec search_key_record key min_index max_index channel =
  let index = (min_index + max_index) / 2 in
  let key_offset = key_offset index in
  Lwt_io.set_position channel key_offset
  >>= fun () ->
  read_key_payload channel
  >>= fun current_key ->
  if key = current_key then
    read_key_record ~key_payload:current_key channel >|= Option.some
  else if min_index = max_index then Lwt.return_none
  else
    let min_index, max_index =
      if key < current_key then (min_index, pred index)
      else (succ index, max_index)
    in
    if min_index <= max_index then
      search_key_record key min_index max_index channel
    else Lwt.return_none

let read_value input_channel {value_type; value_offset; value_size; _} =
  match value_type with
  | Nothing -> Lwt.return Nothing
  | Value _ ->
      Lwt_io.set_position input_channel value_offset
      >>= fun () ->
      read_exactly input_channel (Int32.to_int value_size) >|= fun v -> Value v

let pull_value_from_level (Key key) channel =
  Lwt_io.set_position channel Int64.zero
  >>= fun () ->
  Lwt_io.BE.read_int64 channel
  >|= Int64.to_int
  >>= function
  | 0 -> Lwt.return None
  | records_amount -> (
      let min_index, max_index = (0, records_amount - 1) in
      match%lwt search_key_record key min_index max_index channel with
      | Some key_rec -> read_value channel key_rec >|= fun v -> Some v
      | None -> Lwt.return_none)

let rec pull_value_from_levels key = function
  | [] -> Lwt.return Nothing
  | level :: levels -> (
      Lwt_io.atomic (pull_value_from_level key) level
      >>= function
      | Some v -> Lwt.return v
      | None -> pull_value_from_levels key levels)

let rec pull_value persistent_level key =
  try%lwt
    pull_value_from_levels key persistent_level.levels
    (* In this case "persistent_level.levels" field was updated with new files *)
  with Lwt_io.Channel_closed _ -> pull_value persistent_level key

module KeyRecord = struct
  type t = key_record

  let records_amount_length = records_amount_length

  let end_offset = key_offset

  let read_record = read_key_record ?key_payload:None

  let key {key_payload; _} = key_payload
end

module KeyCursor = Cursor.Make (KeyRecord)
module KC = KeyCursor

let merge_levels ~previous_cursor ~next_cursor ~remove_nothing channel
    read_channel =
  let%lwt () =
    Lwt_io.set_position channel (Int64.of_int records_amount_length)
  in
  let records_amount = ref 0 in
  let rec merge_keys () =
    match%lwt
      Lwt.both
        (KC.current_record previous_cursor)
        (KC.current_record next_cursor)
    with
    | ( Some ({key_payload = prev_key; _} as prev_rec),
        Some ({key_payload = next_key; _} as next_rec) ) ->
        if prev_key = next_key then (
          KC.move_forward previous_cursor;
          KC.move_forward next_cursor;
          write_key_record next_rec)
        else if prev_key < next_key then (
          KC.move_forward previous_cursor;
          write_key_record prev_rec)
        else (
          KC.move_forward next_cursor;
          write_key_record next_rec)
    | Some prev_rec, None ->
        KC.move_forward previous_cursor;
        write_key_record prev_rec
    | None, Some next_rec ->
        KC.move_forward next_cursor;
        write_key_record next_rec
    | None, None -> Lwt.return_unit
  and write_key_record {key_payload; value_type; value_size; _} =
    let%lwt () =
      match (value_type, remove_nothing) with
      | Nothing, true -> Lwt.return_unit
      | Nothing, false
      | Value _, _ ->
          incr records_amount;
          let%lwt () = write_key_payload channel key_payload in
          let value_tag = value_tag value_type in
          let value_offset = Int64.zero in
          let%lwt () = U.write_uint8 channel value_tag in
          let%lwt () = Lwt_io.BE.write_int64 channel value_offset in
          Lwt_io.BE.write_int32 channel value_size
    in
    merge_keys ()
  in
  let%lwt () = merge_keys () in
  Logs.info (fun m -> m "Merged %i keys" !records_amount);
  let%lwt () = Lwt_io.set_position channel Int64.zero in
  let%lwt () = Lwt_io.BE.write_int64 channel (Int64.of_int !records_amount) in
  let%lwt () = Lwt_io.flush channel in
  let value_offset = ref (key_offset !records_amount) in
  let%lwt () =
    let open Int64 in
    let key_length = of_int key_length in
    let value_tag_length = of_int value_tag_length in
    let value_offset_length = of_int value_offset_length in
    for%lwt i = 0 to !records_amount - 1 do
      let offset = key_offset i in
      let%lwt () =
        Lwt_io.set_position channel
          (add offset key_length |> add value_tag_length)
      in
      let%lwt () = Lwt_io.BE.write_int64 channel !value_offset in
      let%lwt () =
        Lwt_io.set_position read_channel
          (add offset key_length |> add value_tag_length
         |> add value_offset_length)
      in
      Lwt_io.BE.read_int32 read_channel
      >|= fun value_size ->
      value_offset := Int64.(add !value_offset (of_int32 value_size))
    done
  in
  Logs.info (fun m ->
      m "Merge values. Total size: %Li bytes"
        (Int64.sub !value_offset (key_offset !records_amount)));
  let%lwt () = Lwt_io.set_position channel (key_offset !records_amount)
  and () = KC.reset previous_cursor
  and () = KC.reset next_cursor
  and result_cursor = KC.make ~file_name:"#result" read_channel in
  let rec write_values () =
    match%lwt KC.current_record result_cursor with
    | Some {key_payload; _} -> (
        KC.move_forward result_cursor;
        match%lwt KC.skip_to next_cursor key_payload with
        | Some next_rec -> write_value next_cursor next_rec
        | None -> (
            match%lwt KC.skip_to previous_cursor key_payload with
            | Some prev_rec -> write_value previous_cursor prev_rec
            | None -> assert false))
    | None -> Lwt.return_unit
  and write_value cursor key_rec =
    let%lwt () =
      KC.with_channel cursor (fun cursor_channel ->
          match%lwt read_value cursor_channel key_rec with
          | Nothing -> Lwt.return_unit
          | Value v -> Lwt_io.write channel v)
    in
    write_values ()
  in
  write_values ()

let compact_levels persistent_table =
  Lwt_mutex.with_lock persistent_table.write_mutex (fun () ->
      let%lwt file_names =
        FU.find_ordered_file_names file_extension persistent_table.directory
      in
      let%lwt cursors =
        FU.FileNames.bindings file_names
        |> List.map snd
        |> Lwt_list.map_p KC.of_file_name
      in
      match KC.smallest_pair None cursors with
      | None -> Lwt.return false
      | Some (previous_cursor, next_cursor) ->
          let temp_file_name = KC.file_name next_cursor ^ "_temp" in
          let remove_nothing = previous_cursor == List.hd cursors in
          Logs.info (fun m ->
              m "Start compaction of '%s' and '%s'. Remove nothing: %b"
                (KC.file_name previous_cursor)
                (KC.file_name next_cursor) remove_nothing);
          let%lwt fd, output_channel =
            FU.open_output_file ~replace:true temp_file_name
          in
          let%lwt () =
            Lwt_io.(
              with_file ~mode:input temp_file_name (fun input_channel ->
                  merge_levels ~previous_cursor ~next_cursor ~remove_nothing
                    output_channel input_channel))
          in
          let%lwt () = Lwt_io.flush output_channel in
          let%lwt () = Lwt_unix.fsync fd in
          let%lwt () = Lwt_list.iter_p KC.close cursors in
          let%lwt () = Lwt_io.close output_channel in
          let%lwt () =
            Lwt_unix.rename temp_file_name (KC.file_name next_cursor)
          in
          let%lwt () = Lwt_unix.unlink (KC.file_name previous_cursor) in
          let%lwt new_levels = open_level_files persistent_table.directory in
          let old_levels = persistent_table.levels in
          persistent_table.levels <- new_levels;
          Lwt_list.iter_p Lwt_io.close old_levels
          >|= fun () ->
          Logs.info (fun m ->
              m "End compaction. Result file: %s" (KC.file_name next_cursor));
          true)
