type command =
  | Set of
      { key : string;
        value : string }
  | Get of {key : string}
  | Delete of {key : string}
  | Flush

type request =
  { id : int;
    command : command }

type value =
  | Nil
  | One of string

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
