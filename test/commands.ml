module A = Alcotest
module P = Flask.Protocol
open Common

let set_get_del session () =
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "foo" "aaa") in
  A.(check cmd_result) "set foo" (one "true") result;
  let%lwt result = request cli (set "bar" "bbbbbb") in
  A.(check cmd_result) "set bar" (one "true") result;
  let%lwt result = request cli (set "baz" "ccccccccc") in
  A.(check cmd_result) "set baz" (one "true") result;
  let%lwt result = request cli (set "foo" "AAA") in
  A.(check cmd_result) "set foo" (one "true") result;
  let%lwt result = request cli (set "bar" "bbbbbb") in
  A.(check cmd_result) "set bar" (one "false") result;
  let%lwt result = request cli (get "bar") in
  A.(check cmd_result) "get bar" (one "bbbbbb") result;
  let%lwt result = request cli (get "no-key") in
  A.(check cmd_result) "get no-key" nil result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "AAA") result;
  let%lwt result = request cli (get "baz") in
  A.(check cmd_result) "get baz" (one "ccccccccc") result;
  let%lwt result = request cli (del "baz") in
  A.(check cmd_result) "del baz" (one "true") result;
  let%lwt result = request cli (get "baz") in
  A.(check cmd_result) "get baz" nil result;
  let%lwt result = request cli (del "bar") in
  A.(check cmd_result) "del bar" (one "true") result;
  let%lwt result = request cli (get "bar") in
  A.(check cmd_result) "get bar" nil result;
  let%lwt result = request cli (set "baz" "X") in
  A.(check cmd_result) "set baz" (one "true") result;
  let%lwt result = request cli (get "baz") in
  A.(check cmd_result) "get baz" (one "X") result;
  let%lwt result = request cli (get "bar") in
  A.(check cmd_result) "get bar" nil result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "AAA") result;
  let%lwt result = request cli (del "no-key") in
  A.(check cmd_result) "del no-key" (one "true") result;
  Lwt.return_unit

let batch_set_get_del session () =
  let%lwt cli = create_client session in
  let%lwt _ = send_request cli (set "foo" "aaa") in
  let%lwt _ = send_request cli (set "bar" "bbbbbb") in
  let%lwt _ = send_request cli (set "baz" "ccccccccc") in
  let%lwt _ = send_request cli (set "foo" "AAA") in
  let%lwt _ = send_request cli (set "bar" "bbbbbb") in
  let%lwt _ = send_request cli (get "bar") in
  let%lwt _ = send_request cli (get "no-key") in
  let%lwt _ = send_request cli (get "foo") in
  let%lwt _ = send_request cli (get "baz") in
  let%lwt _ = send_request cli (del "baz") in
  let%lwt _ = send_request cli (get "baz") in
  let%lwt _ = send_request cli (del "bar") in
  let%lwt _ = send_request cli (get "bar") in
  let%lwt _ = send_request cli (set "baz" "X") in
  let%lwt _ = send_request cli (get "baz") in
  let%lwt _ = send_request cli (get "bar") in
  let%lwt _ = send_request cli (get "foo") in
  let%lwt () = Lwt_unix.sleep 0.005 in
  let expected =
    [ resp 4 (one "false");
      resp 5 (one "bbbbbb");
      resp 7 (one "AAA");
      resp 8 (one "ccccccccc");
      resp 10 nil;
      resp 12 nil;
      resp 14 (one "X");
      resp 15 nil;
      resp 16 (one "AAA");
      resp 6 nil;
      resp 13 (one "true");
      resp 11 (one "true");
      resp 9 (one "true");
      resp 3 (one "true");
      resp 2 (one "true");
      resp 1 (one "true");
      resp 0 (one "true") ]
  in
  A.(check (list response)) "get foo" expected (get_responses cli);
  Lwt.return_unit

let conditional_update session () =
  let%lwt cli = create_client session in
  let%lwt result = request cli (P.Add {key = "foo"; value = "111"}) in
  A.(check cmd_result) "add foo" (one "true") result;
  let%lwt result = request cli (P.Add {key = "foo"; value = "222"}) in
  A.(check cmd_result) "add foo" (one "false") result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "111") result;
  let%lwt result = request cli (P.Replace {key = "bar"; value = "111"}) in
  A.(check cmd_result) "replace bar" (one "false") result;
  let%lwt result = request cli (get "bar") in
  A.(check cmd_result) "get bar" nil result;
  let%lwt result = request cli (P.Replace {key = "foo"; value = "XXX"}) in
  A.(check cmd_result) "replace foo" (one "true") result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "XXX") result;
  let%lwt result =
    request cli (P.CAS {key = "foo"; old_value = "GGG"; new_value = "QQQ"})
  in
  A.(check cmd_result) "compare and swap foo" (one "false") result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "XXX") result;
  let%lwt result =
    request cli (P.CAS {key = "foo"; old_value = "XXX"; new_value = "UUU"})
  in
  A.(check cmd_result) "compare and swap foo" (one "true") result;
  let%lwt result = request cli (get "foo") in
  A.(check cmd_result) "get foo" (one "UUU") result;
  let%lwt result =
    request cli (P.CAS {key = "no-key"; old_value = "5"; new_value = "6"})
  in
  A.(check cmd_result) "compare and swap no-key" (one "true") result;
  let%lwt result = request cli (get "no-key") in
  A.(check cmd_result) "get no-key" (one "6") result;
  let%lwt _ = request cli (set "bar" "JJJ") in
  let%lwt result = request cli (P.Swap {key1 = "foo"; key2 = "bar"}) in
  A.(check cmd_result) "swap foo and bar" (one "true") result;
  let%lwt result =
    Lwt.both (request cli (get "foo")) (request cli (get "bar"))
  in
  A.(check (pair cmd_result cmd_result))
    "get foo and bar"
    (one "JJJ", one "UUU")
    result;
  let%lwt result = request cli (P.Swap {key1 = "foo"; key2 = "zzz"}) in
  A.(check cmd_result) "swap foo and zzz" (one "true") result;
  let%lwt result =
    Lwt.both (request cli (get "foo")) (request cli (get "zzz"))
  in
  A.(check (pair cmd_result cmd_result))
    "get foo and zzz"
    (nil, one "JJJ")
    result;
  Lwt.return_unit

let string_manipulation session () =
  let%lwt cli = create_client session in
  let%lwt result = request cli (set "g" "ABCD") in
  A.(check cmd_result) "set g" (one "true") result;
  let%lwt result = request cli (P.Append {key = "g"; value = "zzz"}) in
  A.(check cmd_result) "append to g" (one "true") result;
  let%lwt result = request cli (P.Prepend {key = "g"; value = "xxx"}) in
  A.(check cmd_result) "prepend to g" (one "true") result;
  let%lwt result = request cli (get "g") in
  A.(check cmd_result) "get g" (one "xxxABCDzzz") result;
  let%lwt result = request cli (P.GetLength {key = "g"}) in
  A.(check cmd_result) "get length of g" (one "10") result;
  let%lwt result =
    request cli (P.GetRange {key = "g"; start = 2; length = 7})
  in
  A.(check cmd_result) "get range of g" (one "xABCDzz") result;
  let%lwt result =
    request cli (P.GetRange {key = "g"; start = 2; length = 70})
  in
  A.(check cmd_result) "get range of g" (error "Invalid range") result;
  let%lwt result = request cli (P.GetLength {key = "no-key"}) in
  A.(check cmd_result) "get length of no-key" (one "0") result;
  let%lwt result = request cli (P.Prepend {key = "zzz"; value = "WWW"}) in
  A.(check cmd_result) "prepend to zzz" (one "true") result;
  let%lwt result = request cli (get "zzz") in
  A.(check cmd_result) "get zzz" (one "WWW") result;
  Lwt.return_unit

let incr_decr session () =
  let%lwt cli = create_client session in
  let%lwt _ = request cli (set "a" "77") in
  let%lwt _ = request cli (set "b" "yyy") in
  let%lwt result = request cli (P.Incr {key = "a"; value = 345}) in
  A.(check cmd_result) "increment a" (one "422") result;
  let%lwt result = request cli (P.Decr {key = "a"; value = 10}) in
  A.(check cmd_result) "decrement a" (one "412") result;
  let%lwt result = request cli (P.Incr {key = "b"; value = 345}) in
  A.(check cmd_result)
    "increment b"
    (error "Current value is not an integer")
    result;
  let%lwt result = Lwt.both (request cli (get "a")) (request cli (get "b")) in
  A.(check (pair cmd_result cmd_result))
    "get a and b"
    (one "412", one "yyy")
    result;
  Lwt.return_unit

let range_query session () =
  let%lwt cli = create_client session in
  let%lwt _ = send_request cli (set "abcd" "aaa") in
  let%lwt _ = send_request cli (set "dd" "aaa") in
  let%lwt _ = send_request cli (set "ddd" "aaa") in
  let%lwt _ = send_request cli (set "d1" "aaa") in
  let%lwt _ = send_request cli (set "d2" "aaa") in
  let%lwt _ = send_request cli (set "d22" "aaa") in
  let%lwt _ = send_request cli (set "d3" "aaa") in
  let%lwt _ = send_request cli (set "d6" "aaa") in
  let%lwt _ = send_request cli (set "ab" "aaa") in
  let%lwt _ = send_request cli (set "bc" "bbbbbb") in
  let%lwt _ = send_request cli (set "cb" "bbbbbb") in
  let%lwt _ = send_request cli (set "ada" "aaa") in
  let%lwt _ = send_request cli (set "a1" "aaa") in
  let%lwt _ = send_request cli (set "a10" "aaa") in
  let%lwt _ = send_request cli (set "bda" "aaa") in
  let%lwt result = request cli (P.Keys {start_key = "ab"; end_key = "cd"}) in
  A.(check cmd_result)
    "keys between ab and cd"
    (many ["ab"; "abcd"; "ada"; "bc"; "bda"; "cb"])
    result;
  let%lwt result = request cli (P.Count {start_key = "ab"; end_key = "cd"}) in
  A.(check cmd_result) "count keys between ab and cd" (one "6") result;
  let%lwt result = request cli (P.Keys {start_key = ""; end_key = "d2"}) in
  A.(check cmd_result)
    "keys up to d2"
    (many ["a1"; "a10"; "ab"; "abcd"; "ada"; "bc"; "bda"; "cb"; "d1"])
    result;
  let%lwt result = request cli (P.Keys {start_key = "c"; end_key = "~"}) in
  A.(check cmd_result)
    "keys from c"
    (many ["cb"; "d1"; "d2"; "d22"; "d3"; "d6"; "dd"; "ddd"])
    result;
  let%lwt result = request cli (P.Keys {start_key = "bc"; end_key = "bc"}) in
  A.(check cmd_result) "keys between bc and bc" (many []) result;
  let%lwt result = request cli (P.Keys {start_key = "bc"; end_key = "bca"}) in
  A.(check cmd_result) "keys between bc and bca" (many ["bc"]) result;
  let%lwt result = request cli (P.Keys {start_key = "d"; end_key = "a"}) in
  A.(check cmd_result)
    "keys between d and a"
    (error "Start key is greater than end key")
    result;
  Lwt.return_unit

let stats session () =
  let%lwt _cli = create_client session in
  let%lwt cli = create_client session in
  let%lwt _ = send_request cli (set "abcd" "aaa") in
  let%lwt _ = send_request cli (set "dd" "aaa") in
  let%lwt _ = send_request cli (set "ddd" "aaa") in
  let%lwt _ = send_request cli (set "d1" "aaa") in
  let%lwt _ = send_request cli (set "d2" "aaa") in
  let%lwt _ = request cli P.Flush in
  let%lwt _ = send_request cli (set "d22" "aaa") in
  let%lwt _ = send_request cli (set "d3" "aaa") in
  let%lwt _ = send_request cli (get "d6") in
  let%lwt _ = send_request cli (set "ab" "aaa") in
  let%lwt _ = request cli P.Flush in
  let%lwt _ = send_request cli (get "bc") in
  let%lwt _ = send_request cli (set "cb" "bbbbbb") in
  let%lwt _ = send_request cli (set "ada" "aaa") in
  let%lwt _ = send_request cli (set "a1" "aaa") in
  let%lwt _ = send_request cli (set "a10" "aaa") in
  let%lwt _ = send_request cli (get "bda") in
  let%lwt result = request cli P.GetStats in
  A.(check cmd_result)
    "stats"
    (many
       [ "memory-table-size";
         "4";
         "log-size-mb";
         "0.000051";
         "persistent-table-levels";
         "2";
         "pending-requests";
         "7";
         "pull-operations";
         "2";
         "read-cache-size";
         "1" ])
    result;
  let%lwt _ = request cli P.Flush in
  let%lwt _ = request cli P.Compact in
  let%lwt _ = request cli P.Compact in
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
  [ test_case "SET-GET-DEL" `Quick set_get_del;
    test_case "batch SET-GET-DEL" `Quick batch_set_get_del;
    test_case "conditional update" `Quick conditional_update;
    test_case "string manipulation" `Quick string_manipulation;
    test_case "INCR-DECR" `Quick incr_decr;
    test_case "range query" `Quick range_query;
    test_case "stats" `Quick stats ]
