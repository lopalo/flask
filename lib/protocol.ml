module U = Util

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

open Lwt.Infix

let write_int64 channel value =
  Lwt_io.BE.write_int64 channel @@ Int64.of_int value

let write_key_value channel key value =
  U.write_short_string channel key
  >>= fun () -> U.write_long_string channel value

let write_key_int channel key value =
  U.write_short_string channel key >>= fun () -> write_int64 channel value

let write_key_key channel key key' =
  U.write_short_string channel key
  >>= fun () -> U.write_short_string channel key'

let write_request channel {id; command} =
  write_int64 channel id
  >>= fun () ->
  U.write_uint8 channel
    (match command with
    | Set _ -> 0
    | Get _ -> 1
    | Delete _ -> 2
    | Flush -> 3
    | Compact -> 4
    | Keys _ -> 5
    | Count _ -> 6
    | Add _ -> 7
    | Replace _ -> 8
    | CAS _ -> 9
    | Swap _ -> 10
    | Append _ -> 11
    | Prepend _ -> 12
    | Incr _ -> 13
    | Decr _ -> 14
    | GetLength _ -> 15
    | GetRange _ -> 16
    | GetStats -> 17)
  >>= fun () ->
  match command with
  | Set {key; value} -> write_key_value channel key value
  | Get {key} -> U.write_short_string channel key
  | Delete {key} -> U.write_short_string channel key
  | Flush -> Lwt.return_unit
  | Compact -> Lwt.return_unit
  | Keys {start_key; end_key} -> write_key_key channel start_key end_key
  | Count {start_key; end_key} -> write_key_key channel start_key end_key
  | Add {key; value} -> write_key_value channel key value
  | Replace {key; value} -> write_key_value channel key value
  | CAS {key; old_value; new_value} ->
      write_key_value channel key old_value
      >>= fun () -> U.write_long_string channel new_value
  | Swap {key1; key2} -> write_key_key channel key1 key2
  | Append {key; value} -> write_key_value channel key value
  | Prepend {key; value} -> write_key_value channel key value
  | Incr {key; value} -> write_key_int channel key value
  | Decr {key; value} -> write_key_int channel key value
  | GetLength {key} -> U.write_short_string channel key
  | GetRange {key; start; length} ->
      U.write_short_string channel key
      >>= fun () ->
      write_int64 channel start >>= fun () -> write_int64 channel length
  | GetStats -> Lwt.return_unit

let write_response channel {id; result} =
  write_int64 channel id
  >>= fun () ->
  match result with
  | Ok result -> (
      U.write_uint8 channel 0
      >>= fun () ->
      U.write_uint8 channel
        (match result with
        | Nil -> 0
        | OneLong _ -> 1
        | ManyShort _ -> 2)
      >>= fun () ->
      match result with
      | Nil -> Lwt.return_unit
      | OneLong s -> U.write_long_string channel s
      | ManyShort ss -> U.write_many_short_strings channel ss)
  | Error (ResponseError error) ->
      U.write_uint8 channel 1 >>= fun () -> U.write_short_string channel error

let write_message message_writer channel msg =
  try%lwt Lwt_io.atomic (fun channel -> message_writer channel msg) channel with
  | Unix.Unix_error (Unix.EPIPE, _, _)
  | Unix.Unix_error (Unix.EBADF, _, _)
  | Unix.Unix_error (Unix.ENOTCONN, _, _)
  | Unix.Unix_error (Unix.ECONNREFUSED, _, _)
  | Unix.Unix_error (Unix.ECONNRESET, _, _)
  | Unix.Unix_error (Unix.ECONNABORTED, _, _) ->
      Lwt.return_unit

let write_request_message = write_message write_request

let write_response_message = write_message write_response

let read_key_value channel =
  U.read_short_string channel
  >>= fun key ->
  U.read_long_string channel >>= fun value -> Lwt.return (key, value)

let read_int64 channel = Lwt_io.BE.read_int64 channel >|= Int64.to_int

let read_key_int channel =
  U.read_short_string channel
  >>= fun key -> read_int64 channel >>= fun i -> Lwt.return (key, i)

let read_key_key channel =
  U.read_short_string channel
  >>= fun key ->
  U.read_short_string channel >>= fun value -> Lwt.return (key, value)

let read_command channel =
  U.read_uint8 channel
  >>= function
  | 0 -> read_key_value channel >|= fun (key, value) -> Set {key; value}
  | 1 -> U.read_short_string channel >|= fun key -> Get {key}
  | 2 -> U.read_short_string channel >|= fun key -> Delete {key}
  | 3 -> Lwt.return Flush
  | 4 -> Lwt.return Compact
  | 5 ->
      read_key_key channel
      >|= fun (start_key, end_key) -> Keys {start_key; end_key}
  | 6 ->
      read_key_key channel
      >|= fun (start_key, end_key) -> Count {start_key; end_key}
  | 7 -> read_key_value channel >|= fun (key, value) -> Add {key; value}
  | 8 -> read_key_value channel >|= fun (key, value) -> Replace {key; value}
  | 9 ->
      read_key_value channel
      >>= fun (key, old_value) ->
      U.read_long_string channel
      >|= fun new_value -> CAS {key; old_value; new_value}
  | 10 -> read_key_key channel >|= fun (key1, key2) -> Swap {key1; key2}
  | 11 -> read_key_value channel >|= fun (key, value) -> Append {key; value}
  | 12 -> read_key_value channel >|= fun (key, value) -> Prepend {key; value}
  | 13 -> read_key_int channel >|= fun (key, value) -> Incr {key; value}
  | 14 -> read_key_int channel >|= fun (key, value) -> Decr {key; value}
  | 15 -> U.read_short_string channel >|= fun key -> GetLength {key}
  | 16 ->
      U.read_short_string channel
      >>= fun key ->
      read_int64 channel
      >>= fun start ->
      read_int64 channel >|= fun length -> GetRange {key; start; length}
  | 17 -> Lwt.return GetStats
  | n -> failwith @@ "Unknown command tag: " ^ string_of_int n

let read_request channel =
  read_int64 channel
  >>= fun id -> read_command channel >>= fun command -> Lwt.return {id; command}

let read_result channel =
  U.read_uint8 channel
  >>= function
  | 0 ->
      U.read_uint8 channel
      >>= (function
            | 0 -> Lwt.return Nil
            | 1 -> U.read_long_string channel >|= fun s -> OneLong s
            | 2 -> U.read_many_short_strings channel >|= fun ss -> ManyShort ss
            | n -> failwith @@ "Unknown value tag: " ^ string_of_int n)
      >|= fun value -> Ok value
  | 1 ->
      U.read_short_string channel >|= fun error -> Error (ResponseError error)
  | n -> failwith @@ "Unknown result tag: " ^ string_of_int n

let read_response channel =
  read_int64 channel
  >>= fun id -> read_result channel >>= fun result -> Lwt.return {id; result}

let read reader channel handler =
  try%lwt
    let rec loop () = reader channel >>= handler >>= loop in
    loop ()
  with
  | End_of_file
  | Unix.Unix_error (Unix.EBADF, _, _)
  | Unix.Unix_error (Unix.ENOTCONN, _, _)
  | Unix.Unix_error (Unix.ECONNREFUSED, _, _)
  | Unix.Unix_error (Unix.ECONNRESET, _, _)
  | Unix.Unix_error (Unix.ECONNABORTED, _, _) ->
      Lwt.return_unit

let read_request = read read_request

let read_response = read read_response
