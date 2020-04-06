open Lwt.Infix
open Common
module U = Util
module FU = FileUtil

type state =
  { directory : string;
    number : int;
    fd : Lwt_unix.file_descr;
    output_channel : Lwt_io.output Lwt_io.channel;
    mutable fsync_task : unit Lwt.t * unit Lwt.u }

type t = state ref

let file_extension = "log"

let write_record log (Key key) value =
  let log_state = !log in
  Lwt_io.atomic
    (fun oc ->
      U.write_short_string oc key
      >>= fun () ->
      U.write_uint8 oc
        (match value with
        | Nothing -> 0
        | Value _ -> 1)
      >>= fun () ->
      (match value with
      | Nothing -> Lwt.return_unit
      | Value v -> U.write_long_string oc v)
      >|= fun () -> fst log_state.fsync_task)
    log_state.output_channel

let record_parser =
  let open Angstrom in
  U.short_string_parser
  >>= fun key ->
  any_uint8
  >>= (function
        | 0 -> return Nothing
        | 1 -> U.long_string_parser >>| fun v -> Value v
        | n -> fail @@ "Unknown value tag: " ^ string_of_int n)
  >>| fun value -> (Key key, value)

let read_records handler input_channel =
  Angstrom_lwt_unix.parse_many record_parser handler input_channel >|= ignore

let read_records directory reader =
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  let read_file file_name =
    Lwt_io.(with_file ~mode:input file_name (read_records reader))
  in
  FU.FileNames.bindings file_names |> List.map snd |> Lwt_list.iter_s read_file

let initialize directory =
  FU.open_next_output_file ~replace:false file_extension directory
  >|= fun {fd; output_channel; number; _} ->
  ref {directory; number; fd; output_channel; fsync_task = Lwt.wait ()}

let synchornize log_state =
  Lwt_io.atomic
    (fun oc ->
      Lwt_io.flush oc
      >>= fun () ->
      let fsync_resolver = snd log_state.fsync_task in
      log_state.fsync_task <- Lwt.wait ();
      Lwt.return fsync_resolver)
    log_state.output_channel
  >>= fun fsync_resolver ->
  Lwt_unix.fsync log_state.fd >|= fun () -> Lwt.wakeup_later fsync_resolver ()

let truncate_files directory current_number () =
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  let prev_file_names, _, _ = FU.FileNames.split current_number file_names in
  prev_file_names |> FU.FileNames.bindings |> List.map snd
  |> Lwt_list.iter_s Lwt_unix.unlink

let advance log switch_callback =
  let dir = !log.directory in
  initialize dir
  >>= fun new_log ->
  let old_log_state = !log in
  let new_log_state = !new_log in
  log := new_log_state;
  switch_callback ();
  synchornize old_log_state
  >>= fun () ->
  Lwt_io.close old_log_state.output_channel
  >|= fun () -> truncate_files dir new_log_state.number

let run_synchronizer (config : Config.t) log =
  let period = U.seconds config.log_fsync_period in
  let rec loop () =
    Lwt_unix.sleep period >>= fun () -> synchornize !log >>= fun () -> loop ()
  in
  loop ()
