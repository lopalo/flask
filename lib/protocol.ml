module U = Util

type command =
  | Set of
      { key : string;
        value : string }
  | Get of {key : string}
  | Delete of {key : string}
  | Flush
  | Compact

type request =
  { id : int;
    command : command }

type value =
  | Nil
  | One of string

(* TODO: *)
(* | Many of string list *)

type error = ResponseError of string [@@unboxed]

type cmd_result = (value, error) result

type response =
  { id : int;
    result : cmd_result }

open Lwt.Infix

let write_request oc {id; command} =
  Lwt_io.BE.write_int64 oc @@ Int64.of_int id
  >>= fun () ->
  U.write_uint8 oc
    (match command with
    | Set _ -> 0
    | Get _ -> 1
    | Delete _ -> 2
    | Flush -> 3
    | Compact -> 4)
  >>= fun () ->
  match command with
  | Set {key; value} ->
      U.write_short_string oc key >>= fun () -> U.write_long_string oc value
  | Get {key} -> U.write_short_string oc key
  | Delete {key} -> U.write_short_string oc key
  | Flush -> Lwt.return_unit
  | Compact -> Lwt.return_unit

let write_response oc {id; result} =
  Lwt_io.BE.write_int64 oc @@ Int64.of_int id
  >>= fun () ->
  match result with
  | Ok result -> (
      U.write_uint8 oc 0
      >>= fun () ->
      U.write_uint8 oc
        (match result with
        | Nil -> 0
        | One _ -> 1)
      >>= fun () ->
      match result with
      | Nil -> Lwt.return_unit
      | One s -> U.write_long_string oc s)
  | Error (ResponseError error) ->
      U.write_uint8 oc 1 >>= fun () -> U.write_short_string oc error

let write_message message_writer output_channel msg =
  Lwt_io.atomic (fun oc -> message_writer oc msg) output_channel

let write_request_message = write_message write_request

let write_response_message = write_message write_response

module Parser = struct
  open Angstrom

  let command =
    any_uint8
    >>= function
    | 0 ->
        (fun key value -> Set {key; value})
        <$> U.Parser.short_string <*> U.Parser.long_string
    | 1 -> U.Parser.short_string >>| fun key -> Get {key}
    | 2 -> U.Parser.short_string >>| fun key -> Delete {key}
    | 3 -> return Flush
    | 4 -> return Compact
    | n -> failwith @@ "Unknown command tag: " ^ string_of_int n

  let request =
    (fun id command -> {id = Int64.to_int id; command})
    <$> BE.any_int64 <*> command

  let result =
    any_uint8
    >>= function
    | 0 ->
        any_uint8
        >>= (function
              | 0 -> return Nil
              | 1 -> U.Parser.long_string >>| fun s -> One s
              | n -> failwith @@ "Unknown value tag: " ^ string_of_int n)
        >>| fun value -> Ok value
    | 1 -> U.Parser.short_string >>| fun error -> Error (ResponseError error)
    | n -> failwith @@ "Unknown result tag: " ^ string_of_int n

  let response =
    (fun id result -> {id = Int64.to_int id; result})
    <$> BE.any_int64 <*> result
end

let read parser input_channel handler =
  try%lwt Angstrom_lwt_unix.parse_many parser handler input_channel >|= ignore
  with Unix.Unix_error (Unix.ECONNRESET, "read", "") -> Lwt.return_unit

let read_request = read Parser.request

let read_response = read Parser.response
