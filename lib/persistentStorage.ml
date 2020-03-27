module IntMap = Map.Make (Int)
open Lwt.Infix
open Common
module U = Util

type log_state =
  { fd : Lwt_unix.file_descr;
    output_channel : Lwt_io.output Lwt_io.channel;
    mutable fsync_waiters : unit Lwt.u list }

type log = log_state ref

let write_log_record log (Key key) value =
  Lwt_io.atomic
    (fun oc ->
      U.write_short_string oc key
      >>= fun () ->
      U.write_uint8 oc
        (match value with
        | Removal -> 0
        | Value _ -> 1)
      >>= fun () ->
      match value with
      | Removal -> Lwt.return_unit
      | Value v -> U.write_long_string oc v)
    !log.output_channel

let log_record_parser =
  let open Angstrom in
  U.short_string_parser
  >>= fun key ->
  any_uint8
  >>= (function
        | 0 -> return Removal
        | 1 -> U.long_string_parser >>| fun v -> Value v
        | n -> fail @@ "Unknown value tag: " ^ string_of_int n)
  >>| fun value -> (Key key, value)

let read_log_records handler input_channel =
  Angstrom_lwt_unix.parse_many log_record_parser handler input_channel
  >|= ignore

let find_ordered_file_names file_extension directory =
  Lwt_stream.fold
    (fun name names ->
      match String.split_on_char '.' name with
      | [number; ext] when ext = file_extension ->
          IntMap.add (int_of_string number)
            (Filename.concat directory name)
            names
      | _ -> names)
    (Lwt_unix.files_of_directory directory)
    IntMap.empty

let read_logs directory reader =
  find_ordered_file_names "log" directory
  >>= fun file_names ->
  let read_log file_name =
    Lwt_io.(with_file ~mode:input file_name (read_log_records reader))
  in
  IntMap.bindings file_names |> List.map snd |> Lwt_list.iter_s read_log

let next_file_number file_names =
  match IntMap.max_binding_opt file_names with
  | None -> 0
  | Some (num, _) -> succ num

let open_next_output_file file_extension directory =
  find_ordered_file_names file_extension directory
  >|= next_file_number
  >|= (fun number -> string_of_int number ^ "." ^ file_extension)
  >>= fun file_name ->
  let path = Filename.concat directory file_name in
  let flags = [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_EXCL; Unix.O_NONBLOCK] in
  Lwt_unix.openfile path flags 0o666
  >>= fun fd -> Lwt.return (fd, Lwt_io.(of_fd ~mode:output fd))

let initialize_log directory =
  open_next_output_file "log" directory
  >|= fun (fd, output_channel) -> ref {fd; output_channel; fsync_waiters = []}

let wait_synchronization log resolver =
  let log_state = !log in
  log_state.fsync_waiters <- resolver :: log_state.fsync_waiters

let synchornize_log log =
  let {fd; output_channel; fsync_waiters} = !log in
  !log.fsync_waiters <- [];
  Lwt_io.flush output_channel
  >>= fun () ->
  Lwt_unix.fsync fd
  >>= fun () ->
  List.iter (fun w -> Lwt.wakeup_later w ()) fsync_waiters;
  Lwt.return_unit

let run_synchronizer (config : Config.t) log =
  let period = U.seconds config.fsync_period in
  let rec loop () =
    Lwt_unix.sleep period
    >>= fun () -> synchornize_log log >>= fun () -> loop ()
  in
  loop ()
