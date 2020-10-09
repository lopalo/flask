open Lwt.Infix
open Common
module U = Util
module FU = FileUtil

type state =
  { directory : string;
    number : int;
    fd : Lwt_unix.file_descr;
    output_channel : Lwt_io.output Lwt_io.channel;
    mutable fsync_is_needed : bool;
    mutable fsync_task : unit Lwt.t * unit Lwt.u }

type t = state ref

let file_extension = "log"

let write_records log pairs =
  let log_state = !log in
  Lwt_io.atomic
    (fun oc ->
      U.write_uint8 oc (List.length pairs)
      >>= fun () ->
      Lwt_list.iter_s
        (fun (Key key, value) ->
          U.write_short_string oc key
          >>= fun () ->
          U.write_uint8 oc
            (match value with
            | Nothing -> 0
            | Value _ -> 1)
          >>= fun () ->
          match value with
          | Nothing -> Lwt.return_unit
          | Value v -> U.write_long_string oc v)
        pairs
      >|= fun () ->
      log_state.fsync_is_needed <- true;
      fst log_state.fsync_task)
    log_state.output_channel

let read_record channel =
  U.read_short_string channel
  >>= fun key ->
  U.read_uint8 channel
  >>= (function
        | 0 -> Lwt.return Nothing
        | 1 -> U.read_long_string channel >|= fun v -> Value v
        | n -> failwith @@ "Unknown value tag: " ^ string_of_int n)
  >|= fun value -> (Key key, value)

let read_records channel =
  U.read_uint8 channel >>= U.read_n_times channel read_record

let read_record_file reader channel =
  let rec f () =
    Lwt.catch
      (fun () -> read_records channel)
      (function
        | End_of_file -> Lwt.return_nil
        | exn -> Lwt.fail exn)
    >>= function
    | [] -> Lwt.return_unit
    | records -> Lwt_list.iter_s reader records >>= f
  in
  f ()

let read_record_files directory reader =
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  let read_file file_name =
    Lwt_io.(with_file ~mode:input file_name (read_record_file reader))
  in
  FU.FileNames.bindings file_names |> List.map snd |> Lwt_list.iter_s read_file

let initialize_state directory =
  FU.open_next_output_file ~replace:false file_extension directory
  >|= fun {fd; output_channel; number; _} ->
  { directory;
    number;
    fd;
    output_channel;
    fsync_is_needed = false;
    fsync_task = Lwt.wait () }

let initialize directory = initialize_state directory >|= ref

let fsync log_state =
  Lwt_io.atomic
    (fun oc ->
      Lwt_io.flush oc
      >>= fun () ->
      if log_state.fsync_is_needed then (
        let fsync_resolver = snd log_state.fsync_task in
        log_state.fsync_is_needed <- false;
        log_state.fsync_task <- Lwt.wait ();
        Lwt.return_some fsync_resolver)
      else Lwt.return_none)
    log_state.output_channel
  >>= function
  | Some fsync_resolver ->
      Lwt_unix.fsync log_state.fd
      >|= fun () -> Lwt.wakeup_later fsync_resolver ()
  | None -> Lwt.return_unit

let truncate_files directory current_number () =
  Logs.info (fun m -> m "Remove log files up to #%i" current_number);
  FU.find_ordered_file_names file_extension directory
  >>= fun file_names ->
  let prev_file_names, _, _ = FU.FileNames.split current_number file_names in
  prev_file_names |> FU.FileNames.bindings |> List.map snd
  |> Lwt_list.iter_s Lwt_unix.unlink

let advance log switch_callback =
  let dir = !log.directory in
  initialize_state dir
  >>= fun new_log_state ->
  Logs.info (fun m -> m "Advance log to #%i segment" new_log_state.number);
  let old_log_state = !log in
  log := new_log_state;
  switch_callback ();
  fsync old_log_state
  >>= fun () ->
  Lwt_io.close old_log_state.output_channel
  >|= fun () -> truncate_files dir new_log_state.number

let synchronize {contents = log} = fsync log

let files_size log =
  FU.find_ordered_file_names file_extension !log.directory
  >>= fun file_names ->
  let add size file_name =
    Lwt_unix.stat file_name >|= fun {st_size; _} -> size + st_size
  in
  FU.FileNames.bindings file_names
  |> List.map snd |> Lwt_list.fold_left_s add 0
  >|= fun size -> {bytes = size}
