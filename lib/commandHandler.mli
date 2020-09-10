open Common

type result =
  | Done of Protocol.cmd_result
  | WaitWriteSync of unit Lwt.t Lwt.t * Protocol.cmd_result
  | PullValues of key list
  | Wait of Protocol.cmd_result Lwt.t

val handle : ServerState.t -> Protocol.command -> result
