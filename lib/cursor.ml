open Lwt.Infix

module type Record = sig
  type t

  val start_offset : int

  val end_offset : int -> Int64.t

  val read_record : Lwt_io.input_channel -> t Lwt.t

  val key : t -> string
end

module Make (Record : Record) = struct
  type t =
    { file_name : string;
      channel : Lwt_io.input_channel;
      end_offset : int64;
      mutable current_record : Record.t option }

  let reset cursor =
    cursor.current_record <- None;
    Lwt_io.set_position cursor.channel (Int64.of_int Record.start_offset)

  let make ~file_name ~channel ~records_amount () =
    let cursor =
      { file_name;
        channel;
        end_offset = Record.end_offset records_amount;
        current_record = None }
    in
    reset cursor >|= fun () -> cursor

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

  val make :
    file_name:string ->
    channel:Lwt_io.input_channel ->
    records_amount:int ->
    unit ->
    t Lwt.t

  val current_record : t -> record option Lwt.t

  val file_name : t -> string

  val move_forward : t -> unit

  val reset : t -> unit Lwt.t

  val with_channel : t -> (Lwt_io.input_channel -> 'a Lwt.t) -> 'a Lwt.t

  val skip_to : t -> string -> record option Lwt.t

  val smallest_pair : (t * t) option -> t list -> (t * t) option

  val close : t -> unit Lwt.t
end
