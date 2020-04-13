type t

val initialize : string -> t Lwt.t

val is_writing : t -> bool

val flush_memory_table :
  t -> PersistentLog.t -> Common.memory_table -> int Lwt.t

val pull_value : t -> Common.key -> Common.value Lwt.t

val compact_levels : t -> bool Lwt.t
