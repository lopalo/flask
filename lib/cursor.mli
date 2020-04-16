module type RecordType = sig
  type t

  val records_amount_length : int

  val end_offset : int -> Int64.t

  val read_record : Lwt_io.input_channel -> t Lwt.t

  val key : t -> string
end

module type S = sig
  type t

  type record

  val make : file_name:string -> Lwt_io.input_channel -> t Lwt.t

  val of_file_name : Lwt_io.file_name -> t Lwt.t

  val current_record : t -> record option Lwt.t

  val file_name : t -> string

  val move_forward : t -> unit

  val reset : t -> unit Lwt.t

  val with_channel : t -> (Lwt_io.input_channel -> 'a Lwt.t) -> 'a Lwt.t

  val skip_to : t -> string -> record option Lwt.t

  val smallest_pair : (t * t) option -> t list -> (t * t) option

  val close : t -> unit Lwt.t
end

module Make (Record : RecordType) : S with type record = Record.t

val read_records_amount : Lwt_io.input_channel -> int Lwt.t
