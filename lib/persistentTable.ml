module H = Hashtbl
open Lwt.Infix
open Common
module U = Util
module FU = FileUtil

type level = Lwt_io.input_channel

type levels = level list

type t = levels ref

type key_record =
  { key_offset : int64;
    value_offset : int64;
    value_size : int32 }

let file_extension = "level"

let records_amount_length = 8

let key_length = max_key_length

let value_offset_length = 8

let value_size_length = 4

let key_record_length = key_length + value_offset_length + value_size_length

let key_offset index =
  Int64.of_int (records_amount_length + (index * key_record_length))

let value_tag_length = 1

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
    let (Key key_str) = key in
    let value_str =
      match H.find storage key with
      | Nothing -> ""
      | Value s -> s
    in
    let value_length = String.length value_str in
    Lwt_io.write channel key_str
    >>= fun () ->
    let skip = Int64.of_int (key_length - String.length key_str) in
    let next_pos = Int64.add (Lwt_io.position channel) skip in
    Lwt_io.set_position channel next_pos
    >>= fun () ->
    Lwt_io.BE.write_int64 channel !value_offset
    >>= fun () ->
    (value_offset :=
       Int64.(add !value_offset (of_int (value_tag_length + value_length))));
    Lwt_io.BE.write_int32 channel (Int32.of_int value_length)
  done
  >>= fun () ->
  for%lwt i = 0 to records_amount - 1 do
    let value = H.find storage keys.(i) in
    U.write_uint8 channel
      (match value with
      | Nothing -> 0
      | Value _ -> 1)
    >>= fun () ->
    match value with
    | Nothing -> Lwt.return_unit
    | Value v -> Lwt_io.write channel v
  done

let flush_memory_table directory persistent_table log memtable =
  let storage = ref (H.create 0) in
  let switch () =
    let s = memtable.storage in
    memtable.storage <- H.create (H.length s);
    memtable.previous_storage <- Some s;
    storage := s
  in
  PersistentLog.advance log switch
  >>= fun truncate_log ->
  FU.next_file_name file_extension directory
  >>= fun (file_name, _) ->
  let temp_file_name = file_name ^ "_temp" in
  FU.open_output_file ~replace:true temp_file_name
  >>= fun (fd, output_channel) ->
  write_records output_channel !storage
  >>= fun () ->
  Lwt_io.flush output_channel
  >>= fun () ->
  Lwt_unix.fsync fd
  >>= fun () ->
  Lwt_io.close output_channel
  >>= fun () ->
  Lwt_unix.rename temp_file_name file_name
  >>= fun () ->
  Lwt_io.(open_file ~mode:input file_name)
  >|= (fun input_channel ->
        persistent_table := input_channel :: !persistent_table;
        memtable.previous_storage <- None)
  >>= truncate_log

let initialize directory =
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  let open_file file_name = Lwt_io.(open_file ~mode:input file_name) in
  FU.FileNames.bindings file_names
  |> List.map snd |> List.rev |> Lwt_list.map_s open_file >|= ref

let read_exactly input_channel length =
  let buffer = Bytes.create length in
  Lwt_io.read_into_exactly input_channel buffer 0 length
  >|= fun () -> Bytes.unsafe_to_string buffer

let read_uint8 input_channel = Lwt_io.read_char input_channel >|= Char.code

let null_char = Char.chr 0

let trim_key key = String.split_on_char null_char key |> List.hd

let rec search_key_record key min_index max_index channel =
  let index = (min_index + max_index) / 2 in
  let key_offset = key_offset index in
  Lwt_io.set_position channel key_offset
  >>= fun () ->
  read_exactly channel key_length
  >|= trim_key
  >>= fun current_key ->
  if key = current_key then
    Lwt_io.BE.read_int64 channel
    >>= fun value_offset ->
    Lwt_io.BE.read_int32 channel
    >>= fun value_size -> Lwt.return_some {key_offset; value_offset; value_size}
  else if min_index = max_index then Lwt.return_none
  else
    let min_index, max_index =
      if key < current_key then (min_index, pred index)
      else (succ index, max_index)
    in
    if min_index <= max_index then
      search_key_record key min_index max_index channel
    else Lwt.return_none

let read_value input_channel offset size =
  Lwt_io.set_position input_channel offset
  >>= fun () ->
  read_uint8 input_channel
  >>= function
  | 0 -> Lwt.return Nothing
  | 1 -> read_exactly input_channel (Int32.to_int size) >|= fun v -> Value v
  | n -> failwith @@ "Unknown value tag: " ^ string_of_int n

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
      | Some {value_offset; value_size; _} ->
          read_value channel value_offset value_size >|= fun v -> Some v
      | None -> Lwt.return_none)

let rec pull_value_from_levels key = function
  | [] -> Lwt.return Nothing
  | level :: levels -> (
      Lwt_io.atomic (pull_value_from_level key) level
      >>= function
      | Some v -> Lwt.return v
      | None -> pull_value_from_levels key levels)

let rec pull_value persistent_level key =
  try%lwt pull_value_from_levels key !persistent_level
  with Lwt_io.Channel_closed _ -> pull_value persistent_level key
