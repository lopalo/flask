type t

val run_server : Config.t -> t Lwt.t

val shutdown_server : t -> unit Lwt.t
