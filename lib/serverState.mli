open Common

type t

val initialize : Config.t -> t Lwt.t

val config : t -> Config.t

val incr_pending_requests : t -> unit

val decr_pending_requests : t -> unit

val set_values : t -> (key * value) list -> unit Lwt.t Lwt.t option

val set_value : t -> key -> value -> unit Lwt.t Lwt.t option

val get_value : t -> key -> value option

val is_persistent_table_writing : t -> bool

val log_size : t -> bytes Lwt.t

val trim_cache : t -> unit

val synchronize_log : t -> unit Lwt.t

val flush_memory_table : t -> int Lwt.t

val compact_persisten_table : t -> bool Lwt.t

val search_key_range : t -> key -> key -> keys Lwt.t

val pull_value : t -> key -> unit Lwt.t

val get_stats : t -> string list Lwt.t
