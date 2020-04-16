type t

val create : ?size:int -> unit -> t

val set_value : t -> Common.key -> Common.value -> bool

val get_value : t -> Common.key -> Common.value option

val search_key_range :
  t -> Common.key -> Common.key -> Common.keys -> Common.keys

val start_flushing : t -> Common.values

val end_flushing : t -> unit
