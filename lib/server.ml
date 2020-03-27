module H = Hashtbl
module P = Protocol
module PS = PersistentStorage
open Lwt.Infix
open Common

type storage = (key, value) H.t

type memory_table =
  { storage : storage;
    (* Must be read-only *)
    previous_storage : storage option }

type state =
  { config : Config.t;
    memory_table : memory_table;
    persistent_log : PS.log;
    persistent_tables : unit;
    pending_requests : (string, value Lwt.u) H.t }

type command_result =
  | Done of P.cmd_result
  | WaitSync of P.cmd_result

(* TODO: *)
(* | FetchValue of key *)

type server = {shutdown : unit Lwt.t Lazy.t}

let initialize_state (config : Config.t) =
  let data_dir = config.data_directory in
  let storage = H.create 4096 in
  PS.read_logs data_dir (fun (key, value) ->
      H.replace storage key value;
      Lwt.return_unit)
  >>= fun () ->
  PS.initialize_log data_dir
  >>= fun persistent_log ->
  let memory_table = {storage; previous_storage = None} in
  let persistent_tables = () in
  let pending_requests = H.create 256 in
  Lwt.return
    {config; memory_table; persistent_log; persistent_tables; pending_requests}

let set_value {memory_table = {storage; _}; persistent_log; _} key value =
  match H.find_opt storage key with
  | Some value' when value' = value -> false
  | Some _
  | None ->
      H.replace storage key value;
      ignore @@ PS.write_log_record persistent_log key value;
      true

let get_value {memory_table = {storage; previous_storage; _}; _} key =
  match H.find_opt storage key with
  | Some _ as v -> v
  | None -> (
    match previous_storage with
    | Some s -> H.find_opt s key
    | None -> None)

let delete_value state key = set_value state key Removal

let handle_command state = function
  | P.Set {key; value} ->
      if set_value state (Key key) (Value value) then WaitSync (Ok P.Nil)
      else Done (Ok P.Nil)
  | Get {key} ->
      Done
        (Ok
           (match get_value state (Key key) with
           | Some (Value s) -> P.One s
           | Some Removal -> Nil
           | None -> Nil))
  | Delete {key} ->
      if delete_value state (Key key) then WaitSync (Ok P.Nil)
      else Done (Ok P.Nil)

let handle_request state oc ({id; command} : P.request) =
  match handle_command state command with
  | Done result -> P.write_response_message oc {id; result}
  | WaitSync result ->
      let promise, resolver = Lwt.task () in
      PS.wait_synchronization state.persistent_log resolver;
      promise >>= fun () -> P.write_response_message oc {id; result}

let connection_handler state (ic, oc) =
  P.read_request ic (handle_request state oc)

let run_server config =
  initialize_state config
  >>= fun state ->
  let synchronizer = PS.run_synchronizer config state.persistent_log in
  let ({host; port; _} : Config.t) = config in
  let address = Lwt_unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
  let conn_handler _ connection = connection_handler state connection in
  Logs_lwt.info (fun m -> m "Listen for connections on %s:%i" host port)
  >>= fun () ->
  Lwt_io.establish_server_with_client_address ~no_close:false address
    conn_handler
  >|= fun tcp_server ->
  { shutdown =
      lazy
        (Lwt.cancel synchronizer;
         Lwt_io.shutdown_server tcp_server) }

let shutdown_server server = Lazy.force server.shutdown
