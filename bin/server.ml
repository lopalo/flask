let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let exception_hook exn =
  let message = Printexc.to_string exn in
  let stack = Printexc.get_backtrace () in
  ignore @@ Logs_lwt.err (fun m -> m "%s%s" message stack)

let () =
  let open Flask.Server in
  (* TODO: read 'log_level' from config or argument *)
  let log_level = Logs.Info in
  Logs.(set_reporter (format_reporter ()));
  Logs.set_level (Some log_level);
  Lwt_engine.(set (new libev ~backend:Ev_backend.epoll ()));
  Lwt.async_exception_hook := exception_hook;
  (* TODO: set config from a file and args *)
  let config = create_config () in
  let _server = run_server config in
  Lwt_main.run (forever ())
