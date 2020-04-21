type command =
  | Set of
      { key : string;
        value : string }
  | Get of {key : string}
  | Delete of {key : string}
  | Flush
  | Compact
  | Keys of
      { start_key : string;
        end_key : string }
  | Count of
      { start_key : string;
        end_key : string }
  | Add of
      { key : string;
        value : string }
  | Replace of
      { key : string;
        value : string }
  | CAS of
      { key : string;
        old_value : string;
        new_value : string }
  | Swap of
      { key1 : string;
        key2 : string }
  | Append of
      { key : string;
        value : string }
  | Prepend of
      { key : string;
        value : string }
  | Incr of
      { key : string;
        value : int }
  | Decr of
      { key : string;
        value : int }
  | GetLength of {key : string}
  | GetRange of
      { key : string;
        start : int;
        length : int }
  | GetStats

type request =
  { id : int;
    command : command }

type value =
  | Nil
  | OneLong of string
  | ManyShort of string list

type error = ResponseError of string [@@unboxed]

type cmd_result = (value, error) result

type response =
  { id : int;
    result : cmd_result }

val write_request_message : Lwt_io.output_channel -> request -> unit Lwt.t

val write_response_message : Lwt_io.output_channel -> response -> unit Lwt.t

val read_request : Lwt_io.input_channel -> (request -> unit Lwt.t) -> unit Lwt.t

val read_response :
  Lwt_io.input_channel -> (response -> unit Lwt.t) -> unit Lwt.t
