type t

val read_records :
  string -> (Common.key * Common.value -> unit Lwt.t) -> unit Lwt.t

val write_records : t -> (Common.key * Common.value) list -> unit Lwt.t Lwt.t

val initialize : string -> t Lwt.t

val advance : t -> (unit -> unit) -> (unit -> unit Lwt.t) Lwt.t

val synchronize : t -> unit Lwt.t

val files_size : t -> Common.bytes Lwt.t
