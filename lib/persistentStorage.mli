type log

val read_logs :
  string -> (Common.key * Common.value -> unit Lwt.t) -> unit Lwt.t

val write_log_record : log -> Common.key -> Common.value -> unit Lwt.t

val initialize_log : string -> log Lwt.t

val wait_synchronization : log -> unit Lwt.u -> unit

val run_synchronizer : Config.t -> log -> 'a Lwt.t
