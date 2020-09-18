module A = Alcotest
module P = Flask.Protocol
open Common

let recovery_from_log session () =
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "a" "aaa") in
  A.(check cmd_result) "set a" (one "true") result;
  let%lwt result = request cli (set "b" "bbb") in
  A.(check cmd_result) "set b" (one "true") result;
  let%lwt result = request cli (set "c" "ccc") in
  A.(check cmd_result) "set c" (one "true") result;
  let%lwt () = kill_server session in
  let%lwt connection_refused =
    try%lwt
      let%lwt _ = create_client session in
      Lwt.return_false
    with Unix.Unix_error (Unix.ECONNREFUSED, "connect", "") -> Lwt.return_true
  in
  A.(check bool) "connection is refused" true connection_refused;
  let%lwt () = start_server session in
  let%lwt () = Lwt_unix.sleep 0.02 in
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "d" "ddd") in
  A.(check cmd_result) "set d" (one "true") result;
  let%lwt result = request cli (del "b") in
  A.(check cmd_result) "del b" (one "true") result;
  let%lwt result = request cli (set "c" "QQQ") in
  A.(check cmd_result) "set c" (one "true") result;
  let%lwt () = kill_server session in
  let%lwt () = start_server session in
  let%lwt () = Lwt_unix.sleep 0.02 in
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "e" "eee") in
  A.(check cmd_result) "set e" (one "true") result;
  let%lwt () = kill_server session in
  let%lwt () = start_server session in
  let%lwt () = Lwt_unix.sleep 0.02 in
  let%lwt cli = create_client session in
  let%lwt result =
    Lwt_list.map_p (fun k -> get k |> request cli) ["a"; "b"; "c"; "d"; "e"]
  in
  A.(check (list cmd_result))
    "get keys a-e"
    [one "aaa"; nil; one "QQQ"; one "ddd"; one "eee"]
    result;
  let%lwt result = request cli (P.Keys {start_key = ""; end_key = "~"}) in
  A.(check cmd_result) "keys" (many ["a"; "c"; "d"; "e"]) result;
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "5";
         "log-size-mb";
         "0.000067";
         "persistent-table-levels";
         "0";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "0" ])
    result;
  let%lwt result = request cli P.Flush in
  A.(check cmd_result) "flush" (one "5") result;
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
         "0" ])
    result;
  Lwt.return_unit

let multi_level_table session () =
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "abc" "1") in
  A.(check cmd_result) "set abc" (one "true") result;
  let%lwt result = request cli (set "bc" "2") in
  A.(check cmd_result) "set bc" (one "true") result;
  let%lwt result = request cli (set "ecd" "3") in
  A.(check cmd_result) "set ecd" (one "true") result;
  let%lwt result = request cli (set "cde" "4") in
  A.(check cmd_result) "set cde" (one "true") result;
  let%lwt result = request cli (set "bb" "5") in
  A.(check cmd_result) "set bb" (one "true") result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result) "keys" (many ["bb"; "bc"; "cde"]) result;
  let%lwt result = request cli P.Flush in
  A.(check cmd_result) "flush" (one "5") result;
  let%lwt result = request cli (get "bc") in
  A.(check cmd_result) "get bc" (one "2") result;
  let%lwt result = request cli (set "b2" "6") in
  A.(check cmd_result) "set b2" (one "true") result;
  let%lwt result = request cli (set "ccc" "CCC") in
  A.(check cmd_result) "set ccc" (one "true") result;
  let%lwt result = request cli (set "b1" "7") in
  A.(check cmd_result) "set b1" (one "true") result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "bb"; "bc"; "ccc"; "cde"])
    result;
  let%lwt result = request cli P.Flush in
  A.(check cmd_result) "flush" (one "3") result;
  let%lwt result = request cli (get "bc") in
  A.(check cmd_result) "get bc" (one "2") result;
  let%lwt result = request cli (set "b3" "8") in
  A.(check cmd_result) "set b3" (one "true") result;
  let%lwt result = request cli (set "b4" "9") in
  A.(check cmd_result) "set b4" (one "true") result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "b3"; "b4"; "bb"; "bc"; "ccc"; "cde"])
    result;
  let%lwt result = request cli P.Flush in
  A.(check cmd_result) "flush" (one "2") result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "b3"; "b4"; "bb"; "bc"; "ccc"; "cde"])
    result;
  let%lwt result = request cli (get "bc") in
  A.(check cmd_result) "get bc" (one "2") result;
  let%lwt result = request cli (get "b3") in
  A.(check cmd_result) "get b3" (one "8") result;
  let%lwt result = request cli (set "bc" "xxx") in
  A.(check cmd_result) "set bc" (one "true") result;
  let%lwt result = request cli (get "ccc") in
  A.(check cmd_result) "get ccc" (one "CCC") result;
  let%lwt result = request cli (del "ccc") in
  A.(check cmd_result) "del ccc" (one "true") result;
  let%lwt result = request cli (set "b5" "10") in
  A.(check cmd_result) "set b5" (one "true") result;
  let%lwt result = request cli (set "b6" "11") in
  A.(check cmd_result) "set b6" (one "true") result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "b3"; "b4"; "b5"; "b6"; "bb"; "bc"; "cde"])
    result;
  let%lwt result = request cli P.Flush in
  A.(check cmd_result) "flush" (one "4") result;
  let%lwt result = request cli (get "bc") in
  A.(check cmd_result) "get bc" (one "xxx") result;
  let%lwt result = request cli (get "b3") in
  A.(check cmd_result) "get b3" (one "8") result;
  let%lwt result = request cli (get "ccc") in
  A.(check cmd_result) "get ccc" nil result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "b3"; "b4"; "b5"; "b6"; "bb"; "bc"; "cde"])
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
         "4";
         "pending-requests";
         "1";
         "pull-operations";
         "0";
         "read-cache-size";
         "3" ])
    result;
  let%lwt result = request cli P.Compact in
  A.(check cmd_result) "compact" (one "true") result;
  let%lwt result = request cli P.Compact in
  A.(check cmd_result) "compact" (one "true") result;
  let%lwt result = request cli P.Compact in
  A.(check cmd_result) "compact" (one "true") result;
  let%lwt result = request cli P.Compact in
  A.(check cmd_result) "compact" (one "false") result;
  let%lwt result = request cli (get "bc") in
  A.(check cmd_result) "get bc" (one "xxx") result;
  let%lwt result = request cli (get "b3") in
  A.(check cmd_result) "get b3" (one "8") result;
  let%lwt result = request cli (get "ccc") in
  A.(check cmd_result) "get ccc" nil result;
  let%lwt result = request cli (P.Keys {start_key = "b"; end_key = "d"}) in
  A.(check cmd_result)
    "keys"
    (many ["b1"; "b2"; "b3"; "b4"; "b5"; "b6"; "bb"; "bc"; "cde"])
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
         "3" ])
    result;
  Lwt.return_unit

let tests =
  [ test_case "recovery from log" `Quick recovery_from_log;
    test_case "multi-level table" `Quick multi_level_table ]
