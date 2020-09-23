open Common
module U = Util

type t =
  { log_level : Logs.level option;
    host : string;
    port : int;
    data_directory : string;
    log_fsync_period : milliseconds;
    log_size_threshold : megabytes;
    read_cache_capacity : int;
    max_command_attempts : int;
    max_concurrent_requests_per_connection : int }

let default =
  { log_level = Some Logs.Info;
    host = "127.0.0.1";
    port = 14777;
    data_directory = "./data/";
    log_fsync_period = {milliseconds = 50};
    log_size_threshold = {megabytes = 10.0};
    read_cache_capacity = 1000;
    max_command_attempts = 10;
    max_concurrent_requests_per_connection = 10 }

let read_config_file file_name =
  Arg.read_arg file_name |> Array.to_list
  |> List.filter (fun line -> String.trim line <> "")
  |> List.map (fun line ->
         let parts =
           String.split_on_char ' ' line |> List.filter (fun s -> s <> "")
         in
         match parts with
         | [parameter; value] -> (parameter, value)
         | _ ->
             Printf.eprintf "Wrong line: %s\n" line;
             exit 2)

let parse_config_file =
  List.fold_left
    (fun config (parameter, value) ->
      let error msg =
        Printf.eprintf "Wrong config file parameter '%s': %s\n" parameter msg;
        exit 2
      in
      let parse_int () =
        match int_of_string_opt value with
        | Some i -> i
        | None -> error "expected an integer"
      in
      let parse_float () =
        match float_of_string_opt value with
        | Some i -> i
        | None -> error "expected a float"
      in
      match parameter with
      | "log-level" -> (
        match Logs.level_of_string value with
        | Ok log_level -> {config with log_level}
        | Error (`Msg msg) -> error msg)
      | "host" -> {config with host = value}
      | "port" -> {config with port = parse_int ()}
      | "data-directory" -> {config with data_directory = value}
      | "log-fsync-period-ms" ->
          {config with log_fsync_period = {milliseconds = parse_int ()}}
      | "log-size-threshold-mb" ->
          {config with log_size_threshold = {megabytes = parse_float ()}}
      | "read-cache-capacity" -> {config with read_cache_capacity = parse_int ()}
      | "max-command-attempts" ->
          {config with max_command_attempts = parse_int ()}
      | "max-concurrent-requests-per-connection" ->
          {config with max_concurrent_requests_per_connection = parse_int ()}
      | parameter ->
          Printf.eprintf "Unknown config file parameter: '%s'\n" parameter;
          exit 2)
    default

let read () =
  let open Arg in
  let log_level = ref None in
  let host = ref None in
  let port = ref None in
  let config_file = ref None in
  let set_log_level str =
    match Logs.level_of_string str with
    | Ok level -> log_level := Some level
    | Error (`Msg msg) -> raise (Bad msg)
  in
  let specs =
    [ ( "--log-level",
        String set_log_level,
        "Log level. Default: " ^ Logs.level_to_string default.log_level );
      ( "-h",
        String (fun h -> host := Some h),
        "Listening IP address. Default: " ^ default.host );
      ( "-p",
        Int (fun p -> port := Some p),
        "Listening port. Default: " ^ Int.to_string default.port ) ]
  in
  let usage = "flask-server [options] /path/to/config" in
  parse specs (fun file_name -> config_file := Some file_name) usage;
  let config =
    ref
      (match !config_file with
      | None -> default
      | Some file_name -> read_config_file file_name |> parse_config_file)
  in
  (match !log_level with
  | Some log_level -> config := {!config with log_level}
  | None -> ());
  (match !host with
  | Some host -> config := {!config with host}
  | None -> ());
  (match !port with
  | Some port -> config := {!config with port}
  | None -> ());
  !config
