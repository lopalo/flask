module H = Hashtbl
module P = Protocol
module U = Util
module MT = MemoryTable
module PL = PersistentLog
module PT = PersistentTable
open Lwt.Infix
open Common
module ReadCache = Lru.M.Make (Key) (Value)

type state =
  { config : Config.t;
    memory_table : MT.t;
    persistent_log : PL.t;
    persistent_table : PT.t;
    pull_operations : (key, unit Lwt.t) H.t;
    read_cache : ReadCache.t }

type command_result =
  | Done of P.cmd_result
  | WaitWriteSync of unit Lwt.t Lwt.t * P.cmd_result
  | PullValue of key
  | Wait of P.cmd_result Lwt.t

type server = {shutdown : unit Lwt.t Lazy.t}

let initialize_state (config : Config.t) =
  let data_dir = config.data_directory in
  let memory_table = MT.create () in
  PL.read_records data_dir (fun (key, value) ->
      ignore @@ MT.set_value memory_table key value;
      Lwt.return_unit)
  >>= fun () ->
  PL.initialize data_dir
  >>= fun persistent_log ->
  PT.initialize data_dir
  >>= fun persistent_table ->
  let pull_operations = H.create 256 in
  let read_cache = ReadCache.create config.read_cache_capacity in
  Lwt.return
    { config;
      memory_table;
      persistent_log;
      persistent_table;
      pull_operations;
      read_cache }

let set_value {memory_table; persistent_log; pull_operations; read_cache; _} key
    value =
  assert (is_valid_key_length key);
  assert (is_valid_value_length value);
  let result =
    if MT.set_value memory_table key value then
      Some (PL.write_record persistent_log key value)
    else None
  in
  ReadCache.remove key read_cache;
  (match H.find_opt pull_operations key with
  | Some pull_op ->
      H.remove pull_operations key;
      Lwt.cancel pull_op
  | None -> ());
  result

let get_from_cache read_cache key =
  let open ReadCache in
  let res = find key read_cache in
  if Option.is_some res then promote key read_cache;
  res

let get_value {memory_table; read_cache; _} key =
  assert (is_valid_key_length key);
  match MT.get_value memory_table key with
  | Some _ as v -> v
  | None -> get_from_cache read_cache key

let delete_value state key = set_value state key Nothing

let is_pt_writing {persistent_table; _} = PT.is_writing persistent_table

let flush {memory_table; persistent_table; persistent_log; _} =
  PT.flush_memory_table persistent_table persistent_log memory_table

let compact {persistent_table; _} = PT.compact_levels persistent_table

let search_key_range {memory_table; persistent_table; _} start_key end_key =
  assert (start_key <= end_key);
  PT.search_key_range persistent_table memory_table start_key end_key

let one_result value =
  Done
    (Ok
       (match value with
       | Nothing -> P.Nil
       | Value v -> OneLong v))

let error message = Done (Error (ResponseError message))

let pt_is_writing_error = error "Persistent table is writing files to disk"

let key_range_error = error "Start key is greater than end key"

let handle_command state = function
  | P.Set {key; value} -> (
    match set_value state (Key key) (Value value) with
    | Some p -> WaitWriteSync (p, Ok P.Nil)
    | None -> one_result Nothing)
  | Get {key} -> (
      let k = Key key in
      match get_value state k with
      | Some value -> one_result value
      | None -> PullValue k)
  | Delete {key} -> (
    match delete_value state (Key key) with
    | Some p -> WaitWriteSync (p, Ok P.Nil)
    | None -> Done (Ok P.Nil))
  | Flush ->
      if is_pt_writing state then pt_is_writing_error
      else
        Wait
          (flush state
          >|= fun records_amount ->
          Ok (P.OneLong (string_of_int records_amount)))
  | Compact ->
      if is_pt_writing state then pt_is_writing_error
      else
        Wait
          (compact state
          >|= fun status -> Ok (P.OneLong (Bool.to_string status)))
  | Keys {start_key; end_key} ->
      if start_key > end_key then key_range_error
      else
        Wait
          (search_key_range state (Key start_key) (Key end_key)
          >|= fun keys ->
          Ok (P.ManyShort (Keys.elements keys |> List.map (fun (Key k) -> k))))
  | Count {start_key; end_key} ->
      if start_key > end_key then key_range_error
      else
        Wait
          (search_key_range state (Key start_key) (Key end_key)
          >|= fun keys -> Ok (P.OneLong (Keys.cardinal keys |> string_of_int)))

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
                   >>= fun value ->
                   (* To make sure that the callback isn't run immediately if promise is fulfilled *)
                   Lwt.pause ()
                   >|= fun () ->
                   match H.find_opt operations key with
                   | Some o when o == !op ->
                       H.remove operations key;
                       ReadCache.add key value state.read_cache
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

let run_read_cache_trimmer {read_cache; _} =
  U.run_periodically {milliseconds = 1000} (fun () ->
      ReadCache.trim read_cache; Lwt.return_unit)

let run_flusher ({config = {log_size_threshold; _}; persistent_log; _} as state)
    =
  U.run_periodically {milliseconds = 1000} (fun () ->
      PL.files_size persistent_log
      >>= fun {bytes} ->
      if bytes > log_size_threshold.bytes then flush state >|= ignore
      else Lwt.return_unit)

let run_server config =
  initialize_state config
  >>= fun state ->
  let log_synchronizer = PL.run_synchronizer config state.persistent_log in
  let read_cache_trimmer = run_read_cache_trimmer state in
  let flusher = run_flusher state in
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
