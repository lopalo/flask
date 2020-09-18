open Lwt.Infix
module A = Alcotest
module P = Flask.Protocol
open Common

let inc key = P.Incr {key; value = 1}

let one_key session () =
  let client_quantity = 100 in
  let request_quantity = 100 in
  let key = "X" in
  let inc_key = inc key in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        List.init request_quantity (Fun.const inc_key)
        |> Lwt_list.map_p (send_request c)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let%lwt result = request cli (get key) in
  A.(check cmd_result)
    "get counter"
    (client_quantity * request_quantity |> string_of_int |> one)
    result;
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "1";
         "log-size-mb";
         "0.113386";
         "persistent-table-levels";
         "0";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "0" ])
    result;
  Lwt.return_unit

let one_key_with_flush session () =
  let client_quantity = 100 in
  let request_quantity = 100 in
  let key = "X" in
  let inc_key = inc key in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        List.init request_quantity Fun.id
        |> Lwt_list.map_p (fun _ ->
               let%lwt _ = send_request c P.Flush in
               let%lwt req_id = send_request c inc_key in
               let%lwt _ = send_request c P.Flush in
               Lwt.return req_id)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let%lwt _ = request cli P.Flush in
  let%lwt result = request cli (get key) in
  A.(check cmd_result)
    "get counter"
    (client_quantity * request_quantity |> string_of_int |> one)
    result;
  Lwt.return_unit

let one_key_with_compaction session () =
  let client_quantity = 100 in
  let request_quantity = 100 in
  let key = "X" in
  let inc_key = inc key in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        List.init request_quantity Fun.id
        |> Lwt_list.map_p (fun _ ->
               let%lwt _ = send_request c P.Compact in
               let%lwt _ = send_request c P.Flush in
               let%lwt req_id = send_request c inc_key in
               let%lwt _ = send_request c P.Compact in
               Lwt.return req_id)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let%lwt _ = request cli P.Compact in
  let%lwt _ = request cli P.Flush in
  let%lwt _ = request cli P.Compact in
  let%lwt result = request cli (get key) in
  A.(check cmd_result)
    "get counter"
    (client_quantity * request_quantity |> string_of_int |> one)
    result;
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "0";
         "log-size-mb";
         "0.000000";
         "persistent-table-levels";
         "1";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "1" ])
    result;
  Lwt.return_unit

let many_keys session () =
  let client_quantity = 100 in
  let keys = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"; "i"; "j"] in
  let keys = List.concat_map (fun c -> List.map (( ^ ) c) keys) keys in
  let inc_keys = List.map inc keys in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        inc_keys
        |> Lwt_list.map_p (send_request c)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let expected_result =
    List.map (string_of_int client_quantity |> one |> Fun.const) keys
  in
  let%lwt result = Lwt_list.map_p (fun k -> get k |> request cli) keys in
  A.(check (list cmd_result)) "get counters" expected_result result;
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "100";
         "log-size-mb";
         "0.104141";
         "persistent-table-levels";
         "0";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "0" ])
    result;
  Lwt.return_unit

let many_keys_with_flush session () =
  let client_quantity = 100 in
  let keys = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"; "i"; "j"] in
  let keys = List.concat_map (fun c -> List.map (( ^ ) c) keys) keys in
  let inc_keys = List.map inc keys in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        inc_keys
        |> Lwt_list.map_p (fun inc_key ->
               let%lwt _ = send_request c P.Flush in
               let%lwt req_id = send_request c inc_key in
               let%lwt _ = send_request c P.Flush in
               Lwt.return req_id)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let%lwt _ = request cli P.Flush in
  let expected_result =
    List.map (string_of_int client_quantity |> one |> Fun.const) keys
  in
  let%lwt result = Lwt_list.map_p (fun k -> get k |> request cli) keys in
  A.(check (list cmd_result)) "get counters" expected_result result;
  Lwt.return_unit

let many_keys_with_compaction session () =
  let client_quantity = 100 in
  let keys = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"; "i"; "j"] in
  let keys = List.concat_map (fun c -> List.map (( ^ ) c) keys) keys in
  let inc_keys = List.map inc keys in
  let%lwt clients =
    List.init client_quantity (Fun.const session)
    |> Lwt_list.map_p Common.create_client
  in
  let%lwt _ =
    Lwt_list.map_p
      (fun c ->
        inc_keys
        |> Lwt_list.map_p (fun inc_key ->
               let%lwt _ = send_request c P.Compact in
               let%lwt _ = send_request c P.Flush in
               let%lwt req_id = send_request c inc_key in
               let%lwt _ = send_request c P.Compact in
               Lwt.return req_id)
        >>= Lwt_list.map_p (wait_response c))
      clients
  in
  let%lwt cli = create_client session in
  let%lwt _ = request cli P.Compact in
  let%lwt _ = request cli P.Flush in
  let%lwt _ = request cli P.Compact in
  let expected_result =
    List.map (string_of_int client_quantity |> one |> Fun.const) keys
  in
  let%lwt result = Lwt_list.map_p (fun k -> get k |> request cli) keys in
  A.(check (list cmd_result)) "get counters" expected_result result;
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "0";
         "log-size-mb";
         "0.000000";
         "persistent-table-levels";
         "1";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "100" ])
    result;
  Lwt.return_unit

let tests =
  [ test_case "one key" `Slow one_key;
    test_case "one key with flush" `Slow one_key_with_flush;
    test_case "one key with compaction" `Slow one_key_with_compaction;
    test_case "many keys" `Slow many_keys;
    test_case "many keys with flush" `Slow many_keys_with_flush;
    test_case "many keys with compaction" `Slow many_keys_with_compaction ]
