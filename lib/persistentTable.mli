type t

val initialize : string -> t Lwt.t

val is_writing : t -> bool

val flush_memory_table : t -> PersistentLog.t -> MemoryTable.t -> int Lwt.t

val pull_value : t -> Common.key -> Common.value Lwt.t

val search_key_range :
  t -> MemoryTable.t -> Common.key -> Common.key -> Common.Keys.t Lwt.t

val compact_levels : t -> bool Lwt.t
