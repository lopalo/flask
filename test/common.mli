open Flask

type session

type client

val config : Config.t

val start_server : session -> unit Lwt.t

val kill_server : session -> unit Lwt.t

val test_case :
  ?config:Config.t ->
  string ->
  Alcotest.speed_level ->
  (session -> unit -> unit Lwt.t) ->
  unit Alcotest_lwt.test_case

val create_client : session -> client Lwt.t

val send_request : client -> Protocol.command -> int Lwt.t

val get_responses : client -> Protocol.response list

val truncate_responses : client -> unit

val wait_response : client -> ?timeout:float -> int -> Protocol.cmd_result Lwt.t

val request :
  client ->
  ?timeout:float ->
  Protocol.command ->
  (Protocol.value, Protocol.error) result Lwt.t

val set : string -> string -> Protocol.command

val get : string -> Protocol.command

val del : string -> Protocol.command

val nil : Protocol.cmd_result

val one : string -> Protocol.cmd_result

val many : string list -> Protocol.cmd_result

val resp : int -> Protocol.cmd_result -> Protocol.response

val cmd_result : Protocol.cmd_result Alcotest.testable

val response : Protocol.response Alcotest.testable
