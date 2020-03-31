module H = Hashtbl
open Lwt.Infix
open Common
module U = Util
module FU = FileUtil

type table = Lwt_io.input_channel

type tables = table list

type t = tables ref

let file_extension = "data"

let records_amount_length = 8

let key_length = max_key_length

let value_offset_length = 8

let value_size_length = 4

let key_record_length = key_length + value_offset_length + value_size_length

let header_length records_amount =
  Int64.of_int (records_amount_length + (records_amount * key_record_length))

let value_tag_length = 1

let write_records channel memtable =
  let records_amount = H.length memtable in
  let empty_key = Key "" in
  let keys = Array.make records_amount empty_key in
  let value_offset = ref (header_length records_amount) in
  (let i = ref 0 in
   H.to_seq_keys memtable |> Seq.iter (fun key -> keys.(!i) <- key; incr i));
  Array.sort compare keys;
  Lwt_io.BE.write_int64 channel (Int64.of_int records_amount)
  >>= fun () ->
  for%lwt i = 0 to records_amount - 1 do
    let key = keys.(i) in
    let (Key key_str) = key in
    let value_str =
      match H.find memtable key with
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
    let value = H.find memtable keys.(i) in
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
