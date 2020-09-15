module A = Alcotest
module ALwt = Alcotest_lwt
module P = Flask.Protocol
open Common

let test _switch () =
  (* TODO: delete *)
  Lwt.return_unit

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

let tests =
  [ test_case "SET-GET-DEL" `Quick set_get_del;
    test_case "batch SET-GET-DEL" `Quick batch_set_get_del;
    ALwt.test_case "conditional update" `Quick test;
    ALwt.test_case "string manipulation" `Quick test;
    ALwt.test_case "INCR-DECR-SWAP" `Quick test;
    ALwt.test_case "range query" `Quick test;
    ALwt.test_case "stats" `Quick test ]
