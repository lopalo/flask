module H = Hashtbl
module P = Protocol
open Lwt.Infix

type config =
  { host : string;
    port : int }

type storage = (string, string) H.t

type state =
  { config : config;
    storage : storage }

let create_config ?(host = "127.0.0.1") ?(port = 14777) () = {host; port}

let create_state config = {config; storage = H.create 256}

let set_value state key value = H.replace state.storage key value

let get_value state key =
  match H.find_opt state.storage key with
  | Some v -> P.String v
  | None -> Nil

let delete_value state key = H.remove state.storage key

let handle_command state = function
  | P.Set {key; value} -> set_value state key value; Ok P.Nil
  | Get {key} -> Ok (get_value state key)
  | Delete {key} -> delete_value state key; Ok P.Nil

let handle_request state fd ({id; command} : P.request) =
  P.write_response_to_fd {id; result = handle_command state command} fd

let connection_handler state fd =
  Lwt.catch
    (fun () -> P.read_request_from_fd (handle_request state fd) fd >|= ignore)
    (function
      | Unix.Unix_error (Unix.ECONNRESET, "read", "") -> Lwt.return ()
      | exn -> Lwt.fail exn)

let run_server config =
  let state = create_state config in
  let {host; port; _} = config in
  let address = Lwt_unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
  let conn_handler _ connection = connection_handler state connection in
  Logs_lwt.info (fun m -> m "Listen for connections on %s:%i" host port)
  >>= fun () ->
  Lwt_io.establish_server_with_client_socket ~no_close:false address
    conn_handler
