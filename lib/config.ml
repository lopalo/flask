open Common
module U = Util

type t =
  { log_level : Logs.level option;
    host : string;
    port : int;
    data_directory : string;
    log_fsync_period : milliseconds;
    log_size_threshold : bytes;
    read_cache_capacity : int }

let read () =
  let open Arg in
  (* TODO: read config file *)
  let config_file = ref "" in
  let log_level = ref (Some Logs.Info) in
  let host = ref "127.0.0.1" in
  let port = ref 14777 in
  let data_directory = ref "./data/" in
  let log_fsync_period_ms = ref 100 in
  let log_size_threshold_mb = ref 10 in
  let read_cache_capacity = ref 1000 in
  let log_level_spec str =
    match Logs.level_of_string str with
    | Ok level -> log_level := level
    | Error (`Msg msg) -> raise (Bad msg)
  in
  let specs =
    [ ( "--log-level",
        String log_level_spec,
        "Log level. Default: " ^ Logs.level_to_string !log_level );
      ("-h", Set_string host, "Listening IP address. Default: " ^ !host);
      ("-p", Set_int port, "Listening port. Default: " ^ Int.to_string !port) ]
  in
  let usage = "flask-server [options] /path/to/config" in
  parse specs (( := ) config_file) usage;
  { log_level = !log_level;
    host = !host;
    port = !port;
    data_directory = !data_directory;
    log_fsync_period = {milliseconds = !log_fsync_period_ms};
    log_size_threshold = U.bytes !log_size_threshold_mb;
    read_cache_capacity = !read_cache_capacity }
