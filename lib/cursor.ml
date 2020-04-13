open Lwt.Infix

module type RecordType = sig
  type t

  val records_amount_length : int

  val end_offset : int -> Int64.t

  val read_record : Lwt_io.input_channel -> t Lwt.t

  val key : t -> string
end

module Make (Record : RecordType) = struct
  type t =
    { file_name : string;
      channel : Lwt_io.input_channel;
      end_offset : int64;
      mutable current_record : Record.t option }

  type record = Record.t

  let make ~file_name channel =
    Lwt_io.set_position channel Int64.zero
    >>= fun () ->
    Lwt_io.BE.read_int64 channel
    >|= Int64.to_int
    >|= fun records_amount ->
    { file_name;
      channel;
      end_offset = Record.end_offset records_amount;
      current_record = None }

  let of_file_name file_name =
    Lwt_io.(open_file ~mode:input file_name) >>= make ~file_name

  let current_record ({channel; end_offset; current_record; _} as cursor) =
    let offset = Lwt_io.position channel in
    assert (offset <= end_offset);
    match current_record with
    | Some _ as r -> Lwt.return r
    | None when offset = end_offset -> Lwt.return_none
    | None ->
        Record.read_record channel
        >|= fun record ->
        let r = Some record in
        cursor.current_record <- r;
        r

  let file_name {file_name; _} = file_name

  let move_forward cursor = cursor.current_record <- None

  let reset cursor =
    cursor.current_record <- None;
    Lwt_io.set_position cursor.channel
      (Int64.of_int Record.records_amount_length)

  let with_channel {channel; _} f =
    let offset = Lwt_io.position channel in
    (f channel) [%lwt.finally Lwt_io.set_position channel offset]

  let rec skip_to cursor key =
    current_record cursor
    >>= function
    | None -> Lwt.return_none
    | Some record ->
        let current_key = Record.key record in
        if current_key = key then Lwt.return_some record
        else if current_key > key then Lwt.return_none
        else (move_forward cursor; skip_to cursor key)

  let size {end_offset; _} = Int64.to_int end_offset

  let rec smallest_pair current = function
    | []
    | [_] ->
        current
    | x :: (y :: _ as tail) -> (
        let next = Some (x, y) in
        match current with
        | None -> smallest_pair next tail
        | Some (cx, cy) ->
            smallest_pair
              (if size x + size y < size cx + size cy then next else current)
              tail)

  let close {channel; _} = Lwt_io.close channel
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
