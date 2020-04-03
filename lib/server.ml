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
    pull_waiters : (key, value Lwt.u) H.t;
    (* TODO: mutable multi-value lru cache *)
    read_cache : (key * value) option }

type command_result =
  | Done of P.cmd_result
  | WaitSync of unit Lwt.t * P.cmd_result
  | PullValue of key

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
  let pull_waiters = H.create 256 in
  let read_cache = None in
  Lwt.return
    { config;
      memory_table;
      persistent_log;
      persistent_table;
      pull_waiters;
      read_cache }

let set_value {memory_table = {storage; _}; persistent_log; _} key value =
  assert (is_valid_key_length key);
  assert (is_valid_value_length value);
  match H.find_opt storage key with
  | Some value' when value' = value -> None
  | Some _
  | None ->
      H.replace storage key value;
      Some (PL.write_record persistent_log key value)

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
      | None -> (
        match read_cache with
        | Some (k, v) when k = key -> Some v
        | _ -> None))
    | None -> (
      match read_cache with
      | Some (k, v) when k = key -> Some v
      | _ -> None))

let delete_value state key = set_value state key Nothing

let flush {config; memory_table; persistent_table; persistent_log; _} =
  let result = string_of_int (H.length memory_table.storage) in
  let p =
    PT.flush_memory_table config.data_directory persistent_table persistent_log
      memory_table
  in
  (p, result)

let result value =
  Done
    (Ok
       (match value with
       | Nothing -> P.Nil
       | Value v -> One v))

let handle_command state = function
  | P.Set {key; value} -> (
    match set_value state (Key key) (Value value) with
    | Some p -> WaitSync (p, Ok P.Nil)
    | None -> result Nothing)
  | Get {key} -> (
      let k = Key key in
      match get_value state k with
      | Some value -> result value
      | None -> PullValue k)
  | Delete {key} -> (
    match delete_value state (Key key) with
    | Some p -> WaitSync (p, Ok P.Nil)
    | None -> Done (Ok P.Nil))
  | Flush ->
      let p, records_amount = flush state in
      WaitSync (p, Ok (P.One records_amount))

let rec handle_request state oc ({id; command} as req : P.request) =
  (* Give up back pressure in favour of multiplexing *)
  ignore
    (match handle_command state command with
    | Done result -> P.write_response_message oc {id; result}
    | WaitSync (promise, result) ->
        promise >>= fun () -> P.write_response_message oc {id; result}
    | PullValue key ->
        let waiters = state.pull_waiters in
        let run_pulling = not (H.mem waiters key) in
        let promise, resolver = Lwt.task () in
        H.add waiters key resolver;
        if run_pulling then
          ignore
            (PT.pull_value state.persistent_table key
            >|= fun value ->
            let key_waiters = H.find_all waiters key in
            let rec remove_all () =
              if H.mem waiters key then (H.remove waiters key; remove_all ())
            in
            remove_all ();
            List.(rev key_waiters |> iter (fun w -> Lwt.wakeup_later w value)));
        promise
        >>= fun value ->
        let state = {state with read_cache = Some (key, value)} in
        handle_request state oc req);
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
