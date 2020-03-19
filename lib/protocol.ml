module F = Faraday

type command =
  | Set of
      { key : string;
        value : string }
  | Get of {key : string}
  | Delete of {key : string}

type request =
  { id : int;
    command : command }

type value =
  | Nil
  | String of string

type error = ResponseError of string [@@unboxed]

type response =
  { id : int;
    result : (value, error) result }

let yield _ = Lwt.return ()

let write_short_string serializer str =
  F.write_uint8 serializer (String.length str);
  F.write_string serializer str

let write_long_string serializer str =
  String.length str |> Int32.of_int |> F.BE.write_uint32 serializer;
  F.write_string serializer str

let serialize_request serializer {id; command} =
  F.BE.write_uint64 serializer @@ Int64.of_int id;
  F.write_uint8 serializer
    (match command with
    | Set _ -> 0
    | Get _ -> 1
    | Delete _ -> 2);
  match command with
  | Set {key; value} ->
      write_short_string serializer key;
      write_long_string serializer value
  | Get {key} -> write_short_string serializer key
  | Delete {key} -> write_short_string serializer key

let serialize_response serializer {id; result} =
  F.BE.write_uint64 serializer @@ Int64.of_int id;
  match result with
  | Ok result -> (
      F.write_uint8 serializer 0;
      F.write_uint8 serializer
        (match result with
        | Nil -> 0
        | String _ -> 1);
      match result with
      | Nil -> ()
      | String s -> write_long_string serializer s)
  | Error (ResponseError error) ->
      F.write_uint8 serializer 1;
      write_short_string serializer error

let write_to_fd serialize_fn data fd =
  let serializer = F.create 512 in
  let writev = Faraday_lwt_unix.writev_of_fd fd in
  serialize_fn serializer data;
  F.close serializer;
  Faraday_lwt.serialize ~yield ~writev serializer

let write_request_to_fd = write_to_fd serialize_request

let write_response_to_fd = write_to_fd serialize_response

open Angstrom

let short_string = any_uint8 >>= fun length -> take length

let long_string = BE.any_int32 >>= fun length -> take (Int32.to_int length)

let command_parser =
  any_uint8
  >>= function
  | 0 -> (fun key value -> Set {key; value}) <$> short_string <*> long_string
  | 1 -> short_string >>| fun key -> Get {key}
  | 2 -> short_string >>| fun key -> Delete {key}
  | n -> fail @@ "Unknown command tag: " ^ string_of_int n

let request_parser =
  (fun id command -> {id = Int64.to_int id; command})
  <$> BE.any_int64 <*> command_parser

let result_parser =
  any_uint8
  >>= function
  | 0 ->
      any_uint8
      >>= (function
            | 0 -> return Nil
            | 1 -> long_string >>| fun s -> String s
            | n -> fail @@ "Unknown result tag: " ^ string_of_int n)
      >>| fun value -> Ok value
  | 1 -> short_string >>| fun error -> Error (ResponseError error)
  | n -> fail @@ "Unknown status tag: " ^ string_of_int n

let response_parser =
  (fun id result -> {id = Int64.to_int id; result})
  <$> BE.any_int64 <*> result_parser

let read_from_fd parser handler fd =
  let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
  Angstrom_lwt_unix.parse_many parser handler input_channel

let read_request_from_fd = read_from_fd request_parser

let read_response_from_fd = read_from_fd response_parser
