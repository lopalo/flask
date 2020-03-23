module H = Hashtbl
module P = Protocol
open Lwt.Infix

type value = Str of string

(* TODO:  *)
(* | Int of int *)
(* | Tombstone *)

type storage = (string, value) H.t

type state =
  { config : Config.t;
    storage : storage }

let create_state config = {config; storage = H.create 4096}

let set_value state key value = H.replace state.storage key value

let get_value state key = H.find_opt state.storage key

let delete_value state key = H.remove state.storage key

let handle_command state = function
  | P.Set {key; value} ->
      set_value state key (Str value);
      Ok P.Nil
  | Get {key} ->
      Ok
        (match get_value state key with
        | Some (Str s) -> P.One s
        (* | Some Tombstone -> Nil *)
        | None -> Nil)
  | Delete {key} -> delete_value state key; Ok P.Nil

let handle_request state oc ({id; command} : P.request) =
  P.write_response_message oc {id; result = handle_command state command}

let connection_handler state (ic, oc) =
  P.read_request ic (handle_request state oc)

let run_server config =
  let state = create_state config in
  let ({host; port; _} : Config.t) = config in
  let address = Lwt_unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
  let conn_handler _ connection = connection_handler state connection in
  Logs_lwt.info (fun m -> m "Listen for connections on %s:%i" host port)
  >>= fun () ->
  Lwt_io.establish_server_with_client_address ~no_close:false address
    conn_handler
