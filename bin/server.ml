let exception_hook exn =
  let message = Printexc.to_string exn in
  let trace = Printexc.get_backtrace () in
  Logs.err (fun m -> m "%s %s" message trace)

let () =
  let config = Flask.Config.read () in
  let open Flask.Server in
  let formatter = Format.std_formatter in
  Logs.(set_reporter (format_reporter ~app:formatter ~dst:formatter ()));
  Logs.set_level config.log_level;
  Lwt_engine.(set (new libev ~backend:Ev_backend.epoll ()));
  Lwt.async_exception_hook := exception_hook;
  let _server = run_server config in
  Lwt_main.run (Flask.Util.forever ())
