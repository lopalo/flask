type t

val initialize : string -> t Lwt.t

val flush_memory_table :
  string -> t -> PersistentLog.t -> Common.memory_table -> unit Lwt.t
