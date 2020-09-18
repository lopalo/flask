open Flask
module P = Protocol

type session =
  { config : Flask.Config.t;
    mutable server_pid : int option;
    mutable clients : client list }

and client =
  { output_channel : Lwt_io.output_channel;
    request_id : int ref;
    mutable responses : P.response list }

let config =
  { Config.default with
    host = "127.0.0.77";
    log_fsync_period = {milliseconds = 1} }

let start_server session =
  assert (Option.is_none session.server_pid);
  match Lwt_unix.fork () with
  | 0 ->
      Logs.(set_level (Some Debug));
      Flask.Server.run_server session.config |> ignore;
      Flask.Util.forever ()
  | pid ->
      session.server_pid <- Some pid;
      Lwt_unix.sleep 0.005

let kill_server session =
  let pid = Option.get session.server_pid in
  session.server_pid <- None;
  Unix.kill pid Sys.sigkill;
  let%lwt _ = Lwt_unix.waitpid [] pid in
  Lwt.return_unit

let start_session config =
  let data_directory =
    Filename.(concat (get_temp_dir_name ()) "flask_test_C5KHXcViWkeuqKlj5U3wuQ")
  in
  let%lwt () = FileUtil.remove_dir data_directory in
  let%lwt _ = Lwt_unix.mkdir data_directory 0o700 in
  let config = Config.{config with data_directory} in
  let s = {config; server_pid = None; clients = []} in
  let%lwt () = start_server s in
  Lwt.return s

let terminate_session session =
  List.iter
    (fun {output_channel; _} ->
      Lwt_io.close output_channel |> Lwt.ignore_result)
    session.clients;
  match session.server_pid with
  | Some _ -> kill_server session
  | None -> Lwt.return_unit

let set_session_cleanup switch session =
  Lwt_switch.add_hook (Some switch) (fun () -> terminate_session session)

let test_case ?(config = config) n s f =
  Alcotest_lwt.test_case n s (fun switch () ->
      let%lwt session = start_session config in
      set_session_cleanup switch session;
      f session ())

let create_client session =
  let {Config.host; port; _} = session.config in
  let%lwt addresses = Lwt_unix.getaddrinfo host (string_of_int port) [] in
  let sockaddr = (List.hd addresses).ai_addr in
  let%lwt input_channel, output_channel = Lwt_io.open_connection sockaddr in
  let cli = {output_channel; request_id = ref 0; responses = []} in
  Lwt.async (fun () ->
      P.read_response input_channel (fun r ->
          cli.responses <- r :: cli.responses;
          Lwt.return_unit));
  Lwt.return cli

let send_request {request_id; output_channel; _} command =
  let module P = P in
  let req_id = !request_id in
  incr request_id;
  let req = {P.id = req_id; command} in
  let%lwt () = P.write_request_message output_channel req in
  Lwt.return req_id

let get_responses {responses; _} = List.rev responses

let truncate_responses client = client.responses <- []

let wait_response client ?(timeout = 1.5) request_id =
  let start_ts = Unix.gettimeofday () in
  let rec f () =
    let res =
      List.find_map
        (fun {P.id; result} -> if request_id = id then Some result else None)
        client.responses
    in
    match res with
    | Some r -> Lwt.return r
    | None ->
        if Unix.gettimeofday () -. start_ts > timeout then
          Lwt.fail_with "timeout"
        else
          let%lwt _ = Lwt_unix.sleep 0.001 in
          f ()
  in
  f ()

let request client ?timeout command =
  let%lwt req_id = send_request client command in
  wait_response client ?timeout req_id

let set key value = P.Set {key; value}

let get key = P.Get {key}

let del key = P.Delete {key}

let nil = Ok P.Nil

let one value = Ok (P.OneLong value)

let many values = Ok (P.ManyShort values)

let error message = Error (P.ResponseError message)

let resp id result = {P.id; result}

let value_pp ppf = function
  | P.Nil -> Fmt.string ppf "Nil"
  | OneLong v -> Fmt.string ppf v
  | ManyShort vs -> Fmt.(Dump.list string) ppf vs

let error_pp ppf (P.ResponseError err) = Fmt.string ppf err

let cmd_result_pp = Fmt.result ~ok:value_pp ~error:error_pp

let response_pp ppf {P.id; result} =
  Fmt.string ppf "#"; Fmt.int ppf id; Fmt.sp ppf (); cmd_result_pp ppf result

let cmd_result = Alcotest.testable cmd_result_pp ( = )

let response = Alcotest.testable response_pp ( = )
