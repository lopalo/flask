let exception_hook exn =
  let message = Printexc.to_string exn in
  let trace = Printexc.get_backtrace () in
  Logs.err (fun m -> m "%s %s" message trace)

let log_reporter =
  let open Logs in
  let ppf = Format.std_formatter in
  let report _src level ~over k msgf =
    let k _ = over (); k () in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    let t = Unix.(time () |> gmtime) in
    let ts =
      Format.sprintf "%04i-%02i-%02i %02i:%02i:%02i" (t.tm_year + 1900)
        (succ t.tm_mon) t.tm_mday t.tm_hour t.tm_min t.tm_sec
    in
    Format.kfprintf k ppf
      ("%s %a @[" ^^ fmt ^^ "@]@.")
      ts pp_header (level, header)
  in
  {report}

let () =
  let config = Flask.Config.read () in
  let open Flask.Server in
  Logs.(set_reporter log_reporter);
  Logs.set_level config.log_level;
  Lwt_engine.(set (new libev ~backend:Ev_backend.epoll ()));
  Lwt.async_exception_hook := exception_hook;
  let _server = run_server config in
  Lwt_main.run (Flask.Util.forever ())
