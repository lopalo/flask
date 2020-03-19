type config

val create_config : ?host:string -> ?port:int -> unit -> config

val run_server : config -> Lwt_io.server Lwt.t
