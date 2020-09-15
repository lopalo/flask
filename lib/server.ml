module Proto = Protocol
module State = ServerState
open Lwt.Infix
open Common

type t = {shutdown : unit Lwt.t Lazy.t}

let rec handle_request state oc attempt ({id; command} as req : Proto.request) =
  if attempt < (State.config state).max_command_attempts then
    match CommandHandler.handle state command with
    | Done result -> Proto.write_response_message oc {id; result}
    | WaitWriteSync (write_promise, result) ->
        write_promise
        >>= fun sync_promise ->
        sync_promise >>= fun () -> Proto.write_response_message oc {id; result}
    | PullValues keys ->
        List.map (State.pull_value state) keys
        |> Lwt.join
        >>= fun () -> handle_request state oc (succ attempt) req
    | Wait promise ->
        promise >>= fun result -> Proto.write_response_message oc {id; result}
  else
    Proto.write_response_message oc
      { id;
        result =
          Error (ResponseError "Too many attempts to execute the command") }

let connection_handler state (ic, oc) =
  let pool =
    Lwt_pool.create (State.config state).max_concurrent_requests_per_connection
      (fun () -> Lwt.return_unit)
  in
  let req_handler request =
    State.incr_pending_requests state;
    let wait =
      (Lwt_pool.use pool (fun () -> handle_request state oc 0 request))
        [%lwt.finally Lwt.return (State.decr_pending_requests state)]
    in
    (* Put back pressure on a client if there're too many concurrent requests from it *)
    if Lwt_pool.wait_queue_length pool > 0 then wait else Lwt.return_unit
  in
  Proto.read_request ic req_handler

let run_read_cache_trimmer state =
  Util.run_periodically {milliseconds = 1000} (fun () ->
      State.trim_cache state; Lwt.return_unit)

let run_log_synchronizer state =
  let period = (State.config state).log_fsync_period in
  Util.run_periodically period (fun () -> State.synchronize_log state)

let run_memory_table_flusher state =
  let threshold = Util.bytes (State.config state).log_size_threshold in
  Util.run_periodically {milliseconds = 1000} (fun () ->
      State.log_size state
      >>= fun {bytes} ->
      if bytes > threshold.bytes then State.flush_memory_table state >|= ignore
      else Lwt.return_unit)

let run_server config =
  State.initialize config
  >>= fun state ->
  let log_synchronizer = run_log_synchronizer state in
  let read_cache_trimmer = run_read_cache_trimmer state in
  let flusher = run_memory_table_flusher state in
  let ({host; port; _} : Config.t) = config in
  let address = Lwt_unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
  let conn_handler _ connection = connection_handler state connection in
  Logs.info (fun m -> m "Listen for connections on %s:%i" host port);
  Lwt_io.establish_server_with_client_address ~no_close:false address
    conn_handler
  >|= fun tcp_server ->
  { shutdown =
      lazy
        (Lwt.cancel log_synchronizer;
         Lwt.cancel read_cache_trimmer;
         Lwt.cancel flusher;
         Lwt_io.shutdown_server tcp_server) }

let shutdown_server server = Lazy.force server.shutdown
