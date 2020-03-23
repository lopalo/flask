type t =
  { log_level : Logs.level;
    host : string;
    port : int }

let create ?(log_level = Logs.Info) ?(host = "127.0.0.1") ?(port = 14777) () =
  {log_level; host; port}
