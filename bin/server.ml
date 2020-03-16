let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let () =
  let open Flask.Server in
  let config = create_config () in
  let _server = run_server config in
  Lwt_main.run (forever ())
