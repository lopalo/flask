module H = Hashtbl

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
  | Some v -> v
  | None -> "nil"

let delete_value state key = H.remove state.storage key

let handle_command state = function
  | ["set"; key; value] -> set_value state key value; "Ok"
  | ["get"; key] -> get_value state key
  | ["del"; key] -> delete_value state key; "Ok"
  | _ -> "Invalid command"

let rec connection_handler state ((input, output) as channel) =
  let%lwt line = Lwt_io.read_line input in
  let command =
    String.split_on_char ' ' line |> List.filter (fun s -> s <> "")
  in
  let response = handle_command state command in
  let%lwt _ = Lwt_io.write_line output response in
  connection_handler state channel

let run_server config =
  let state = create_state config in
  let address =
    Lwt_unix.ADDR_INET (Unix.inet_addr_of_string config.host, config.port)
  in
  let conn_handler _ channel = connection_handler state channel in
  Lwt_io.establish_server_with_client_address address conn_handler
