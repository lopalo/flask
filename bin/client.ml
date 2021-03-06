module P = Flask.Protocol

let format = Printf.sprintf

let write_line line =
  Lwt_io.(
    let%lwt () = write_line stdout line in
    flush stdout)

type relay =
  { take : unit -> unit Lwt.t;
    pass : unit -> unit Lwt.t }

let make_relay () =
  let open Lwt_mvar in
  let mv = create () in
  let mv' = create_empty () in
  ( {take = (fun () -> take mv); pass = put mv'},
    {take = (fun () -> take mv'); pass = put mv} )

let dummy_relay =
  let f () = Lwt.return_unit in
  let r = {pass = f; take = f} in
  (r, r)

let handle_response write_prefix relay ({id; result} : P.response) =
  let%lwt () = relay.take () in
  let number = format "#%i" id in
  let status, content =
    match result with
    | Ok value -> (
        ( "OK",
          match value with
          | Nil -> "nil"
          | OneLong s -> format "\"%s\"" s
          | ManyShort ss -> List.map (format "\"%s\"") ss |> String.concat " "
        ))
    | Error (ResponseError err) -> ("Error", err)
  in
  let%lwt () = write_line (format "%s %s %s" number status content) in
  let%lwt () = write_prefix () in
  relay.pass ()

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
  | ["flush"] -> Some Flush
  | ["compact"] -> Some Compact
  | ["keys"; start_key; end_key] -> Some (Keys {start_key; end_key})
  | ["count"; start_key; end_key] -> Some (Count {start_key; end_key})
  | ["add"; key; value] -> Some (Add {key; value})
  | ["replace"; key; value] -> Some (Replace {key; value})
  | ["cas"; key; old_value; new_value] -> Some (CAS {key; old_value; new_value})
  | ["swap"; key1; key2] -> Some (Swap {key1; key2})
  | ["append"; key; value] -> Some (Append {key; value})
  | ["prepend"; key; value] -> Some (Prepend {key; value})
  | ["incr"; key; value] -> (
    match int_of_string_opt value with
    | Some value when value >= 0 -> Some (Incr {key; value})
    | _ -> None)
  | ["decr"; key; value] -> (
    match int_of_string_opt value with
    | Some value when value >= 0 -> Some (Decr {key; value})
    | _ -> None)
  | ["get-length"; key] -> Some (GetLength {key})
  | ["get-range"; key; start; length] -> (
    match (int_of_string_opt start, int_of_string_opt length) with
    | Some start, Some length when start >= 0 && length >= 0 ->
        Some (GetRange {key; start; length})
    | _ -> None)
  | ["get-stats"] -> Some GetStats
  | _ -> None

let run_client ~is_interactive ~wait_response host port =
  let%lwt addresses = Lwt_unix.getaddrinfo host port [] in
  let sockaddr = (List.hd addresses).ai_addr in
  Lwt_io.with_connection sockaddr (fun (ic, oc) ->
      let request_id = ref 0 in
      let relay, relay' =
        if wait_response then make_relay () else dummy_relay
      in
      let write_prefix () =
        if is_interactive then
          Lwt_io.(write stdout (format "flask #%i> " !request_id))
        else Lwt.return_unit
      in
      let reader = P.response_reader ic (handle_response write_prefix relay') in
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
                  let%lwt () = relay.take () in
                  let%lwt () =
                    if is_interactive then Lwt.return_unit
                    else write_line (format "#%i %s" !request_id line)
                  in
                  let request : P.request = {id = !request_id; command} in
                  incr request_id;
                  let%lwt () = P.write_request_message oc request in
                  relay.pass ()
              | None ->
                  let%lwt () = write_line "Invalid command" in
                  write_prefix ()
          in
          loop ()
      in
      (loop ()) [%lwt.finally Lwt.cancel reader; Lwt.return_unit])

let () =
  let open Arg in
  let is_interactive = ref false in
  let wait_response = ref false in
  let host = ref "localhost" in
  let port = ref "14777" in
  let specs =
    [ ("-i", Set is_interactive, "Interactive dialogue mode");
      ("-w", Set wait_response, "Wait for response before sending next request");
      ("-h", Set_string host, "Server host. Default: " ^ !host);
      ("-p", Set_string port, "Server port. Default: " ^ !port) ]
  in
  let usage = "flask-cli [options]" in
  parse specs (fun arg -> raise @@ Bad ("Unexpected argument: " ^ arg)) usage;
  Lwt_main.run
    (run_client ~is_interactive:!is_interactive ~wait_response:!wait_response
       !host !port)
