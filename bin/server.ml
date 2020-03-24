let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let exception_hook exn =
  let message = Printexc.to_string exn in
  let stack = Printexc.get_backtrace () in
  ignore @@ Logs_lwt.err (fun m -> m "%s%s" message stack)

let () =
  let config = Flask.Config.read () in
  let open Flask.Server in
  Logs.(set_reporter (format_reporter ()));
  Logs.set_level config.log_level;
  Lwt_engine.(set (new libev ~backend:Ev_backend.epoll ()));
  Lwt.async_exception_hook := exception_hook;
  let _server = run_server config in
  Lwt_main.run (forever ())
