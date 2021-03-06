module H = Hashtbl
open Lwt.Infix
open Common
module U = Util
module FU = FileUtil
module MT = MemoryTable

module BloomFilter : sig
  type t

  val create : int -> t

  val serialize : t -> Bytes.t

  val deserialize : int -> Bytes.t -> t

  val add : t -> string -> unit

  val check : t -> string -> bool
end = struct
  type t =
    { number_of_hashes : int;
      bitv : Bitv.t }

  let error_rate = 0.01

  let seed = 63922006

  let estimate_parameters number_of_keys =
    let log2 = log 2. in
    let nf = float_of_int number_of_keys in
    let number_of_bits = ceil (-.nf *. log error_rate /. log (2. ** log2)) in
    let number_of_hashes = ceil (log2 *. number_of_bits /. nf) in
    (Int.of_float number_of_bits, Int.of_float number_of_hashes)

  let create number_of_keys =
    let number_of_bits, number_of_hashes = estimate_parameters number_of_keys in
    let bitv = Bitv.create number_of_bits false in
    {number_of_hashes; bitv}

  let serialize {bitv; _} = Bitv.to_bytes bitv

  let deserialize number_of_keys bytes =
    let number_of_bits, number_of_hashes = estimate_parameters number_of_keys in
    let bitv = Bitv.of_bytes bytes in
    if number_of_bits <> Bitv.length bitv then
      failwith "Inconsistent bloom filter";
    {number_of_hashes; bitv}

  let bucket number_of_bits key i =
    let hash = Hashtbl.seeded_hash (seed * i) key in
    hash mod number_of_bits

  let add {number_of_hashes; bitv} key =
    let number_of_bits = Bitv.length bitv in
    let rec f = function
      | 0 -> ()
      | i ->
          Bitv.set bitv (bucket number_of_bits key i) true;
          f (pred i)
    in
    f number_of_hashes

  let check {number_of_hashes; bitv} key =
    let number_of_bits = Bitv.length bitv in
    let rec f = function
      | 0 -> true
      | i ->
          if Bitv.get bitv (bucket number_of_bits key i) then f (pred i)
          else false
    in
    f number_of_hashes
end

type level =
  { channel : Lwt_io.input_channel;
    records_amount : int;
    bloom_filter : BloomFilter.t;
    start_key : key;
    end_key : key }

type t =
  { directory : string;
    write_mutex : Lwt_mutex.t;
    mutable levels : level list }

type key_record =
  { key_payload : string;
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

let write_records channel values =
  let records_amount = H.length values in
  let empty_key = Key "" in
  let keys = Array.make records_amount empty_key in
  let value_offset = ref (key_offset records_amount) in
  (let i = ref 0 in
   H.to_seq_keys values |> Seq.iter (fun key -> keys.(!i) <- key; incr i));
  Array.sort compare keys;
  let bloom_filter = BloomFilter.create records_amount in
  let start_key = if records_amount > 0 then keys.(0) else empty_key in
  let end_key =
    if records_amount > 0 then keys.(records_amount - 1) else empty_key
  in
  Lwt_io.BE.write_int64 channel (Int64.of_int records_amount)
  >>= fun () ->
  for%lwt i = 0 to records_amount - 1 do
    let key = keys.(i) in
    let (Key key_payload) = key in
    let value = H.find values key in
    BloomFilter.add bloom_filter key_payload;
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
    match H.find values keys.(i) with
    | Nothing -> Lwt.return_unit
    | Value v -> Lwt_io.write channel v
  done
  >>= fun () ->
  let bf = BloomFilter.serialize bloom_filter |> Bytes.unsafe_to_string in
  U.write_long_string channel bf
  >>= fun () ->
  (let (Key start_key) = start_key in
   U.write_short_string channel start_key)
  >>= fun () ->
  (let (Key end_key) = end_key in
   U.write_short_string channel end_key)
  >|= fun () -> (records_amount, bloom_filter, start_key, end_key)

let flush_memory_table persistent_table log memtable =
  Lwt_mutex.with_lock persistent_table.write_mutex (fun () ->
      let values = ref (H.create 0) in
      let switch () = values := MT.start_flushing memtable in
      PersistentLog.advance log switch
      >>= fun truncate_log ->
      Logs.info (fun m -> m "Start flushing of %i records" (H.length !values));
      FU.next_file_name file_extension persistent_table.directory
      >>= fun (file_name, _) ->
      let temp_file_name = file_name ^ "_temp" in
      FU.open_output_file ~replace:true temp_file_name
      >>= fun (fd, channel) ->
      write_records channel !values
      >>= fun (records_amount, bloom_filter, start_key, end_key) ->
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
            let level =
              { channel = input_channel;
                records_amount;
                bloom_filter;
                start_key;
                end_key }
            in
            persistent_table.levels <- level :: persistent_table.levels;
            MT.end_flushing memtable)
      >>= truncate_log
      >|= fun () ->
      Logs.info (fun m -> m "End flushing. Result file: %s" file_name);
      H.length !values)

let read_records_amount channel =
  Lwt_io.set_position channel Int64.zero
  >>= fun () -> Lwt_io.BE.read_int64 channel >|= Int64.to_int

let is_writing {write_mutex; _} = Lwt_mutex.is_locked write_mutex

let levels_amount {levels; _} = List.length levels

let null_char = Char.chr 0

let trim_key key = String.split_on_char null_char key |> List.hd

let read_key_payload input_channel =
  U.read_exactly input_channel key_length >|= trim_key

let read_value_type input_channel =
  U.read_uint8 input_channel
  >|= function
  | 0 -> Nothing
  | 1 -> Value ""
  | n -> failwith @@ "Unknown value tag: " ^ string_of_int n

let read_key_record channel =
  read_key_payload channel
  >>= fun key_payload ->
  read_value_type channel
  >>= fun value_type ->
  Lwt_io.BE.read_int64 channel
  >>= fun value_offset ->
  Lwt_io.BE.read_int32 channel
  >>= fun value_size ->
  Lwt.return {key_payload; value_type; value_offset; value_size}

let make_level channel =
  read_records_amount channel
  >>= fun records_amount ->
  records_amount - 1 |> key_offset
  |> Lwt_io.set_position channel
  >>= fun () ->
  read_key_record channel
  >>= fun {value_offset; value_size; _} ->
  Lwt_io.set_position channel Int64.(add value_offset (of_int32 value_size))
  >>= fun () ->
  U.read_long_string channel
  >>= fun bloom_filter_string ->
  U.read_short_string channel
  >>= fun start_key ->
  U.read_short_string channel
  >>= fun end_key ->
  let bloom_filter =
    BloomFilter.deserialize records_amount
      (Bytes.unsafe_of_string bloom_filter_string)
  in
  let start_key = Key start_key in
  let end_key = Key end_key in
  Lwt.return {channel; records_amount; bloom_filter; start_key; end_key}

let open_level_files directory =
  (* From the newest to the oldest *)
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  FU.FileNames.bindings file_names
  |> List.map snd |> List.rev
  |> Lwt_list.map_p (fun file_name ->
         Lwt_io.(open_file ~mode:input file_name) >>= make_level)

let initialize directory =
  open_level_files directory
  >|= fun levels -> {directory; levels; write_mutex = Lwt_mutex.create ()}

let rec search_key_offset key min_index max_index channel =
  let index = (min_index + max_index) / 2 in
  let key_offset = key_offset index in
  Lwt_io.set_position channel key_offset
  >>= fun () ->
  read_key_payload channel
  >>= fun current_key ->
  if key = current_key then Lwt.return_ok key_offset
  else if min_index = max_index then Lwt.return_error key_offset
  else
    let min_index, max_index =
      if key < current_key then (min_index, pred index)
      else (succ index, max_index)
    in
    if min_index <= max_index then
      search_key_offset key min_index max_index channel
    else Lwt.return_error key_offset

let read_value input_channel {value_type; value_offset; value_size; _} =
  match value_type with
  | Nothing -> Lwt.return Nothing
  | Value _ ->
      Lwt_io.set_position input_channel value_offset
      >>= fun () ->
      U.read_exactly input_channel (Int32.to_int value_size)
      >|= fun v -> Value v

let pull_value_from_level {channel; records_amount; bloom_filter; _} (Key key) =
  if records_amount > 0 && BloomFilter.check bloom_filter key then
    let min_index, max_index = (0, records_amount - 1) in
    match%lwt search_key_offset key min_index max_index channel with
    | Ok key_offset ->
        Lwt_io.set_position channel key_offset
        >>= fun () ->
        read_key_record channel
        >>= fun key_rec -> read_value channel key_rec >|= fun v -> Some v
    | Error _ -> Lwt.return_none
  else Lwt.return_none

let rec pull_value_from_levels key = function
  | [] -> Lwt.return Nothing
  | level :: levels -> (
      Lwt_io.atomic
        (fun channel -> pull_value_from_level {level with channel} key)
        level.channel
      >>= function
      | Some v -> Lwt.return v
      | None -> pull_value_from_levels key levels)

let rec pull_value persistent_table key =
  try%lwt
    let levels = persistent_table.levels in
    let%lwt result = pull_value_from_levels key levels in
    (* Check if "persistent_table.levels" has been changed *)
    if levels == persistent_table.levels then Lwt.return result
    else pull_value persistent_table key
  with Lwt_io.Channel_closed _ -> pull_value persistent_table key

let search_key_range_in_level
    { channel;
      records_amount;
      start_key = Key level_start;
      end_key = Key level_end;
      _ } (Key start_key) (Key end_key) =
  if records_amount > 0 && end_key >= level_start && start_key <= level_end then
    let min_index, max_index = (0, records_amount - 1) in
    match%lwt search_key_offset start_key min_index max_index channel with
    | Ok start_offset
    | Error start_offset ->
        let end_offset = key_offset records_amount in
        let open Int64 in
        let value_offset_length = of_int value_offset_length in
        let value_size_length = of_int value_size_length in
        let rec loop ~keys ~deleted_keys =
          let return () =
            Lwt.return (Keys.of_list keys, Keys.of_list deleted_keys)
          in
          if Lwt_io.position channel < end_offset then
            read_key_payload channel
            >>= fun key ->
            if key < end_key then
              read_value_type channel
              >>= fun value_type ->
              let k = Key key in
              let keys, deleted_keys =
                if key < start_key then (keys, deleted_keys)
                else
                  match value_type with
                  | Nothing -> (keys, k :: deleted_keys)
                  | Value _ -> (k :: keys, deleted_keys)
              in
              Lwt_io.set_position channel
                (Lwt_io.position channel |> add value_offset_length
               |> add value_size_length)
              >>= fun () -> loop ~keys ~deleted_keys
            else return ()
          else return ()
        in
        Lwt_io.set_position channel start_offset
        >>= fun () -> loop ~keys:[] ~deleted_keys:[]
  else Lwt.return (Keys.empty, Keys.empty)

let rec search_key_range_in_levels start_key end_key result = function
  | [] -> Lwt.return result
  | level :: levels ->
      Lwt_io.atomic
        (fun channel ->
          search_key_range_in_level {level with channel} start_key end_key)
        level.channel
      >>= fun (keys, deleted_keys) ->
      let result = Keys.diff result deleted_keys |> Keys.union keys in
      search_key_range_in_levels start_key end_key result levels

let rec search_key_range persistent_table memtable start_key end_key =
  try%lwt
    (* From the oldest to the newest *)
    let levels = persistent_table.levels in
    let%lwt result =
      search_key_range_in_levels start_key end_key Keys.empty (List.rev levels)
    in
    (* Check if "persistent_table.levels" has been changed *)
    if levels == persistent_table.levels then
      MT.search_key_range memtable start_key end_key result |> Lwt.return
    else search_key_range persistent_table memtable start_key end_key
  with Lwt_io.Channel_closed _ ->
    search_key_range persistent_table memtable start_key end_key

module KeyRecord = struct
  type t = key_record

  let start_offset = records_amount_length

  let end_offset = key_offset

  let read_record = read_key_record

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
    (* Write value offsets into new key records *)
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
  and result_cursor =
    KC.make ~file_name:"#result" ~channel:read_channel
      ~records_amount:!records_amount ()
  in
  let bloom_filter = BloomFilter.create !records_amount in
  let start_key = ref None in
  let end_key = ref None in
  let rec write_values () =
    match%lwt KC.current_record result_cursor with
    | Some {key_payload; _} -> (
        BloomFilter.add bloom_filter key_payload;
        (match !start_key with
        | Some _ -> ()
        | None -> start_key := Some key_payload);
        end_key := Some key_payload;
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
  let%lwt () = write_values () in
  let bf = BloomFilter.serialize bloom_filter |> Bytes.unsafe_to_string in
  let%lwt () = U.write_long_string channel bf in
  let%lwt () =
    U.write_short_string channel (Option.value ~default:"" !start_key)
  in
  U.write_short_string channel (Option.value ~default:"" !end_key)

let make_cursor file_name =
  Lwt_io.(open_file ~mode:input file_name)
  >>= fun channel ->
  read_records_amount channel
  >>= fun records_amount -> KC.make ~file_name ~channel ~records_amount ()

let compact_levels persistent_table =
  Lwt_mutex.with_lock persistent_table.write_mutex (fun () ->
      let%lwt file_names =
        FU.find_ordered_file_names file_extension persistent_table.directory
      in
      let%lwt cursors =
        FU.FileNames.bindings file_names
        |> List.map snd |> Lwt_list.map_p make_cursor
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
          Lwt_list.iter_p (fun {channel; _} -> Lwt_io.close channel) old_levels
          >|= fun () ->
          Logs.info (fun m ->
              m "End compaction. Result file: %s" (KC.file_name next_cursor));
          true)
