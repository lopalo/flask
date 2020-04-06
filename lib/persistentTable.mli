type t

val initialize : string -> t Lwt.t

val flush_memory_table :
  string -> t -> PersistentLog.t -> Common.memory_table -> int Lwt.t

val pull_value : t -> Common.key -> Common.value Lwt.t
