module H = Hashtbl
module P = Protocol
module PL = PersistentLog
module PT = PersistentTable
open Lwt.Infix
open Common

type state =
  { config : Config.t;
    memory_table : memory_table;
    persistent_log : PL.t;
    persistent_table : PT.t;
    pull_operations : (key, unit Lwt.t) H.t;
    (* TODO: mutable multi-value lru cache *)
    read_cache : (key, value) H.t }

type command_result =
  | Done of P.cmd_result
  | WaitWriteSync of unit Lwt.t Lwt.t * P.cmd_result
  | PullValue of key
  | Wait of P.cmd_result Lwt.t

type server = {shutdown : unit Lwt.t Lazy.t}

let initialize_state (config : Config.t) =
  let data_dir = config.data_directory in
  let storage = H.create 4096 in
  PL.read_records data_dir (fun (key, value) ->
      H.replace storage key value;
      Lwt.return_unit)
  >>= fun () ->
  PL.initialize data_dir
  >>= fun persistent_log ->
  PT.initialize data_dir
  >>= fun persistent_table ->
  let memory_table = {storage; previous_storage = None} in
  let pull_operations = H.create 256 in
  let read_cache = H.create 4096 in
  Lwt.return
    { config;
      memory_table;
      persistent_log;
      persistent_table;
      pull_operations;
      read_cache }

let set_value
    {memory_table = {storage; _}; persistent_log; pull_operations; read_cache; _}
    key value =
  assert (is_valid_key_length key);
  assert (is_valid_value_length value);
  let result =
    match H.find_opt storage key with
    | Some value' when value' = value -> None
    | Some _
    | None ->
        H.replace storage key value;
        Some (PL.write_record persistent_log key value)
  in
  H.remove read_cache key;
  (match H.find_opt pull_operations key with
  | Some pull_op ->
      H.remove pull_operations key;
      Lwt.cancel pull_op
  | None -> ());
  result

let get_value {memory_table = {storage; previous_storage; _}; read_cache; _} key
    =
  assert (is_valid_key_length key);
  match H.find_opt storage key with
  | Some _ as v -> v
  | None -> (
    match previous_storage with
    | Some s -> (
      match H.find_opt s key with
      | Some _ as v -> v
      | None -> H.find_opt read_cache key)
    | None -> H.find_opt read_cache key)

let delete_value state key = set_value state key Nothing

let flush {config; memory_table; persistent_table; persistent_log; _} =
  PT.flush_memory_table config.data_directory persistent_table persistent_log
    memory_table

let result value =
  Done
    (Ok
       (match value with
       | Nothing -> P.Nil
       | Value v -> One v))

let handle_command state = function
  | P.Set {key; value} -> (
    match set_value state (Key key) (Value value) with
    | Some p -> WaitWriteSync (p, Ok P.Nil)
    | None -> result Nothing)
  | Get {key} -> (
      let k = Key key in
      match get_value state k with
      | Some value -> result value
      | None -> PullValue k)
  | Delete {key} -> (
    match delete_value state (Key key) with
    | Some p -> WaitWriteSync (p, Ok P.Nil)
    | None -> Done (Ok P.Nil))
  | Flush ->
      Wait
        (flush state
        >|= fun records_amount -> Ok (P.One (string_of_int records_amount)))

let rec handle_request state oc ({id; command} as req : P.request) =
  (* Give up back pressure in favour of multiplexing *)
  Lwt.async (fun () ->
      match handle_command state command with
      | Done result -> P.write_response_message oc {id; result}
      | WaitWriteSync (write_promise, result) ->
          write_promise
          >>= fun sync_promise ->
          sync_promise >>= fun () -> P.write_response_message oc {id; result}
      | PullValue key ->
          let operations = state.pull_operations in
          let operation =
            match H.find_opt operations key with
            | Some operation -> operation
            | None ->
                let op = ref Lwt.return_unit in
                (op :=
                   PT.pull_value state.persistent_table key
                   >|= fun value ->
                   match H.find_opt operations key with
                   | Some o when o == !op ->
                       H.remove operations key;
                       H.replace state.read_cache key value
                   | Some _
                   | None ->
                       ());
                H.replace operations key !op;
                !op
          in
          (try%lwt operation with Lwt.Canceled -> Lwt.return_unit)
          >>= fun () -> handle_request state oc req
      | Wait promise ->
          promise >>= fun result -> P.write_response_message oc {id; result});
  Lwt.return_unit

let connection_handler state (ic, oc) =
  P.read_request ic (handle_request state oc)

let run_server config =
  initialize_state config
  >>= fun state ->
  let synchronizer = PL.run_synchronizer config state.persistent_log in
  let ({host; port; _} : Config.t) = config in
  let address = Lwt_unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
  let conn_handler _ connection = connection_handler state connection in
  Logs.info (fun m -> m "Listen for connections on %s:%i" host port);
  Lwt_io.establish_server_with_client_address ~no_close:false address
    conn_handler
  >|= fun tcp_server ->
  { shutdown =
      lazy
        (Lwt.cancel synchronizer;
         Lwt_io.shutdown_server tcp_server) }

let shutdown_server server = Lazy.force server.shutdown
