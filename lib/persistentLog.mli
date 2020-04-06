type t

val read_records :
  string -> (Common.key * Common.value -> unit Lwt.t) -> unit Lwt.t

val write_record : t -> Common.key -> Common.value -> unit Lwt.t Lwt.t

val initialize : string -> t Lwt.t

val advance : t -> (unit -> unit) -> (unit -> unit Lwt.t) Lwt.t

val run_synchronizer : Config.t -> t -> 'a Lwt.t
