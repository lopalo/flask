type t =
  { log_level : Logs.level option;
    host : string;
    port : int;
    data_directory : string;
    log_fsync_period : Common.milliseconds;
    log_size_threshold : Common.megabytes;
    read_cache_capacity : int;
    max_command_attempts : int;
    max_concurrent_requests_per_connection : int }

val read : unit -> t

val default : t
