type server

val run_server : Config.t -> server Lwt.t

val shutdown_server : server -> unit Lwt.t
