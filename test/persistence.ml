module ALwt = Alcotest_lwt

let test _switch () = Lwt.return_unit

let tests =
  [ ALwt.test_case "SET-GET-DEL" `Quick test;
    ALwt.test_case "range query" `Quick test;
    ALwt.test_case "stats" `Quick test ]
