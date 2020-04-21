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

let write_int64 oc value = Lwt_io.BE.write_int64 oc @@ Int64.of_int value

let write_key_value oc key value =
  U.write_short_string oc key >>= fun () -> U.write_long_string oc value

let write_key_int oc key value =
  U.write_short_string oc key >>= fun () -> write_int64 oc value

let write_key_key oc key key' =
  U.write_short_string oc key >>= fun () -> U.write_short_string oc key'

let write_request oc {id; command} =
  write_int64 oc id
  >>= fun () ->
  U.write_uint8 oc
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
  | Set {key; value} -> write_key_value oc key value
  | Get {key} -> U.write_short_string oc key
  | Delete {key} -> U.write_short_string oc key
  | Flush -> Lwt.return_unit
  | Compact -> Lwt.return_unit
  | Keys {start_key; end_key} -> write_key_key oc start_key end_key
  | Count {start_key; end_key} -> write_key_key oc start_key end_key
  | Add {key; value} -> write_key_value oc key value
  | Replace {key; value} -> write_key_value oc key value
  | CAS {key; old_value; new_value} ->
      write_key_value oc key old_value
      >>= fun () -> U.write_long_string oc new_value
  | Swap {key1; key2} -> write_key_key oc key1 key2
  | Append {key; value} -> write_key_value oc key value
  | Prepend {key; value} -> write_key_value oc key value
  | Incr {key; value} -> write_key_int oc key value
  | Decr {key; value} -> write_key_int oc key value
  | GetLength {key} -> U.write_short_string oc key
  | GetRange {key; start; length} ->
      U.write_short_string oc key
      >>= fun () -> write_int64 oc start >>= fun () -> write_int64 oc length
  | GetStats -> Lwt.return_unit

let write_response oc {id; result} =
  write_int64 oc id
  >>= fun () ->
  match result with
  | Ok result -> (
      U.write_uint8 oc 0
      >>= fun () ->
      U.write_uint8 oc
        (match result with
        | Nil -> 0
        | OneLong _ -> 1
        | ManyShort _ -> 2)
      >>= fun () ->
      match result with
      | Nil -> Lwt.return_unit
      | OneLong s -> U.write_long_string oc s
      | ManyShort ss -> U.write_many_short_strings oc ss)
  | Error (ResponseError error) ->
      U.write_uint8 oc 1 >>= fun () -> U.write_short_string oc error

let write_message message_writer output_channel msg =
  Lwt_io.atomic (fun oc -> message_writer oc msg) output_channel

let write_request_message = write_message write_request

let write_response_message = write_message write_response

module Parser = struct
  open Angstrom

  let int64 = BE.any_int64 >>| Int64.to_int

  let pair x y = (x, y)

  let key_value = pair <$> U.Parser.short_string <*> U.Parser.long_string

  let key_int = pair <$> U.Parser.short_string <*> int64

  let key_key = pair <$> U.Parser.short_string <*> U.Parser.short_string

  let command =
    any_uint8
    >>= function
    | 0 -> key_value >>| fun (key, value) -> Set {key; value}
    | 1 -> U.Parser.short_string >>| fun key -> Get {key}
    | 2 -> U.Parser.short_string >>| fun key -> Delete {key}
    | 3 -> return Flush
    | 4 -> return Compact
    | 5 -> key_key >>| fun (start_key, end_key) -> Keys {start_key; end_key}
    | 6 -> key_key >>| fun (start_key, end_key) -> Count {start_key; end_key}
    | 7 -> key_value >>| fun (key, value) -> Add {key; value}
    | 8 -> key_value >>| fun (key, value) -> Replace {key; value}
    | 9 ->
        key_value
        >>= fun (key, old_value) ->
        U.Parser.long_string >>| fun new_value -> CAS {key; old_value; new_value}
    | 10 -> key_key >>| fun (key1, key2) -> Swap {key1; key2}
    | 11 -> key_value >>| fun (key, value) -> Append {key; value}
    | 12 -> key_value >>| fun (key, value) -> Prepend {key; value}
    | 13 -> key_int >>| fun (key, value) -> Incr {key; value}
    | 14 -> key_int >>| fun (key, value) -> Decr {key; value}
    | 15 -> U.Parser.short_string >>| fun key -> GetLength {key}
    | 16 ->
        U.Parser.short_string
        >>= fun key ->
        int64
        >>= fun start -> int64 >>| fun length -> GetRange {key; start; length}
    | 17 -> return GetStats
    | n -> failwith @@ "Unknown command tag: " ^ string_of_int n

  let request = (fun id command -> {id; command}) <$> int64 <*> command

  let result =
    any_uint8
    >>= function
    | 0 ->
        any_uint8
        >>= (function
              | 0 -> return Nil
              | 1 -> U.Parser.long_string >>| fun s -> OneLong s
              | 2 -> U.Parser.many_short_strings >>| fun ss -> ManyShort ss
              | n -> failwith @@ "Unknown value tag: " ^ string_of_int n)
        >>| fun value -> Ok value
    | 1 -> U.Parser.short_string >>| fun error -> Error (ResponseError error)
    | n -> failwith @@ "Unknown result tag: " ^ string_of_int n

  let response = (fun id result -> {id; result}) <$> int64 <*> result
end

let read parser input_channel handler =
  try%lwt Angstrom_lwt_unix.parse_many parser handler input_channel >|= ignore
  with Unix.Unix_error (Unix.ECONNRESET, "read", "") -> Lwt.return_unit

let read_request = read Parser.request

let read_response = read Parser.response
