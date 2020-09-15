let () =
  Lwt_main.run
  @@ Alcotest_lwt.run "flask"
       [ ("commands", Commands.tests);
         ("persistence", Persistence.tests);
         ("concurrency", Concurrency.tests) ]
