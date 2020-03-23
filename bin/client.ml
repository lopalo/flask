module P = Flask.Protocol

let format = Printf.sprintf

let stdout = Lwt_io.stdout

let write = Lwt_io.write stdout

let write_line = Lwt_io.write_line stdout

let handle_response write_prefix ({id; result} : P.response) =
  let%lwt () = write (format "#%i " id) in
  let%lwt () =
    match result with
    | Ok value -> (
        let%lwt () = write "OK " in
        match value with
        | Nil -> write "nil"
        | One s -> write (format "\"%s\"" s))
    | Error (ResponseError err) -> write (format "Error %s" err)
  in
  let%lwt () = write "\n" in
  write_prefix ()

let make_command line =
  let parts = String.split_on_char ' ' line |> List.filter (fun s -> s <> "") in
  let parts =
    match parts with
    | cmd :: args -> String.lowercase_ascii cmd :: args
    | x -> x
  in
  match parts with
  | ["set"; key; value] -> Some (P.Set {key; value})
  | ["get"; key] -> Some (Get {key})
  | ["del"; key] -> Some (Delete {key})
  | _ -> None

let connect host port =
  let%lwt addresses = Lwt_unix.getaddrinfo host port [] in
  let sockaddr = (List.hd addresses).ai_addr in
  (Lwt_io.with_connection sockaddr (fun (ic, oc) ->
       let request_id = ref 0 in
       let write_prefix () = write (format "flask #%i> " !request_id) in
       ignore @@ P.read_response ic (handle_response write_prefix);
       let%lwt () = write_prefix () in
       let rec loop () =
         let%lwt line = Lwt_io.read_line Lwt_io.stdin in
         if Lwt_io.is_closed oc then Lwt.return_unit
         else
           let%lwt () =
             if line = "" then Lwt.bind (write_prefix ()) loop
             else
               match make_command line with
               | Some command ->
                   let request : P.request = {id = !request_id; command} in
                   incr request_id;
                   P.write_request_message oc request
               | None ->
                   let%lwt () = write_line "Invalid command" in
                   write_prefix ()
           in
           loop ()
       in
       loop ()))
    [%lwt.finally write_line "Connection closed"]

let () =
  (* TODO: read from args *)
  let host = "localhost" in
  let port = "14777" in
  Lwt_main.run (connect host port)
