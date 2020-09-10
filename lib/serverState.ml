module H = Hashtbl
module MT = MemoryTable
module PL = PersistentLog
module PT = PersistentTable
open Lwt.Infix
open Common
module ReadCache = Lru.M.Make (Key) (Value)

type t =
  { config : Config.t;
    memory_table : MT.t;
    persistent_log : PL.t;
    persistent_table : PT.t;
    pull_operations : (key, unit Lwt.t) H.t;
    read_cache : ReadCache.t;
    pending_requests : int ref }

let set_values_max_amount = 255

let initialize (config : Config.t) =
  let data_dir = config.data_directory in
  let memory_table = MT.create () in
  PL.read_records data_dir (fun (key, value) ->
      ignore @@ MT.set_value memory_table key value;
      Lwt.return_unit)
  >>= fun () ->
  Logs.info (fun m ->
      m "%i records have been restored from log" (MT.size memory_table));
  PL.initialize data_dir
  >>= fun persistent_log ->
  PT.initialize data_dir
  >>= fun persistent_table ->
  let pull_operations = H.create 256 in
  let read_cache = ReadCache.create config.read_cache_capacity in
  let pending_requests = ref 0 in
  Lwt.return
    { config;
      memory_table;
      persistent_log;
      persistent_table;
      pull_operations;
      read_cache;
      pending_requests }

let config {config; _} = config

let incr_pending_requests {pending_requests; _} = incr pending_requests

let decr_pending_requests {pending_requests; _} = decr pending_requests

let set_values {memory_table; persistent_log; pull_operations; read_cache; _}
    pairs =
  assert (List.length pairs <= set_values_max_amount);
  List.iter
    (fun (key, value) ->
      assert (is_valid_key_length key);
      assert (is_valid_value_length value))
    pairs;
  let result =
    let updated_pairs =
      List.filter
        (fun (key, value) -> MT.set_value memory_table key value)
        pairs
    in
    match updated_pairs with
    | [] -> None
    | updated_pairs -> Some (PL.write_records persistent_log updated_pairs)
  in
  List.iter
    (fun (key, _) ->
      ReadCache.remove key read_cache;
      match H.find_opt pull_operations key with
      | Some pull_op ->
          H.remove pull_operations key;
          Lwt.cancel pull_op
      | None -> ())
    pairs;
  result

let set_value state key value = set_values state [(key, value)]

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

let is_persistent_table_writing {persistent_table; _} =
  PT.is_writing persistent_table

let log_size {persistent_log; _} = PL.files_size persistent_log

let trim_cache {read_cache; _} = ReadCache.trim read_cache

let synchronize_log {persistent_log; _} = PL.synchronize persistent_log

let flush_memory_table {memory_table; persistent_table; persistent_log; _} =
  PT.flush_memory_table persistent_table persistent_log memory_table

let compact_persisten_table {persistent_table; _} =
  PT.compact_levels persistent_table

let search_key_range {memory_table; persistent_table; _} start_key end_key =
  assert (start_key <= end_key);
  PT.search_key_range persistent_table memory_table start_key end_key

let pull_value state key =
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
  try%lwt operation with Lwt.Canceled -> Lwt.return_unit

let get_stats
    { memory_table;
      persistent_log;
      persistent_table;
      pull_operations;
      read_cache;
      pending_requests;
      _ } =
  let istr = string_of_int in
  let fstr f = Printf.sprintf "%f" f in
  PL.files_size persistent_log
  >|= (fun log_bytes ->
        ["log-size-mb"; (Util.megabytes log_bytes).megabytes |> fstr])
  >|= fun log_size ->
  let memtable_size = ["memory-table-size"; MT.size memory_table |> istr] in
  let pt_levels =
    ["persistent-table-levels"; PT.levels_amount persistent_table |> istr]
  in
  let pull_operations = ["pull-operations"; H.length pull_operations |> istr] in
  let read_cache_size =
    ["read-cache-size"; ReadCache.size read_cache |> istr]
  in
  let pending_requests = ["pending-requests"; !pending_requests |> istr] in
  let stats =
    [ memtable_size;
      log_size;
      pt_levels;
      pending_requests;
      pull_operations;
      read_cache_size ]
  in
  List.concat stats
