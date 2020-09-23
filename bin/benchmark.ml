module P = Flask.Protocol
module TimeBuckets = Map.Make (Int)

type command_type =
  | Set
  | Get

let bucket_values = [0; 5; 15; 30; 60; 100; 200; 400; 1000; 2000; 3600000]

let run_client ~host ~port ~request_quantity ~data_size ~key_space_size
    ~command_type ~request_counter ~request_time_buckets () =
  let%lwt addresses = Lwt_unix.getaddrinfo host port [] in
  let sockaddr = (List.hd addresses).ai_addr in
  let request_timestamps = Hashtbl.create 500 in
  let task, task_resolver = Lwt.wait () in
  let handle_response {P.id; result} =
    let end_ts = Unix.gettimeofday () in
    let start_ts = Hashtbl.find request_timestamps id in
    Hashtbl.remove request_timestamps id;
    if Result.is_error result then failwith "Failed request";
    let req_time_ms = (end_ts -. start_ts) *. 1000.0 |> int_of_float in
    let bucket_value =
      List.fold_left
        (fun prev curr -> if req_time_ms <= prev then prev else curr)
        0 bucket_values
    in
    TimeBuckets.find bucket_value request_time_buckets |> incr;
    if Hashtbl.length request_timestamps = 0 then
      Lwt.wakeup_later task_resolver ();
    Lwt.return_unit
  in
  Lwt_io.with_connection sockaddr (fun (ic, oc) ->
      let reader = P.read_response ic handle_response in
      let rec loop () =
        let req_id = !request_counter in
        if req_id < request_quantity then (
          incr request_counter;
          let key = Random.int key_space_size |> string_of_int in
          let value = String.make data_size 'J' in
          let command =
            match command_type with
            | Set -> P.Set {key; value}
            | Get -> P.Get {key}
          in
          let request : P.request = {id = req_id; command} in
          let start_ts = Unix.gettimeofday () in
          Hashtbl.add request_timestamps req_id start_ts;
          let%lwt () = P.write_request_message oc request in
          loop ())
        else if Hashtbl.length request_timestamps > 0 then task
        else Lwt.return_unit
      in
      let%lwt () = loop () in
      Lwt.cancel reader; Lwt.return_unit)

let run_benchmark ~client_quantity ~host ~port ~request_quantity ~data_size
    ~key_space_size command_type =
  let request_counter = ref 0 in
  let request_time_buckets =
    TimeBuckets.(
      List.fold_left
        (fun buckets bucket_value -> add bucket_value (ref 0) buckets)
        empty bucket_values)
  in
  let start_ts = Unix.gettimeofday () in
  let%lwt _ =
    List.init client_quantity (Fun.const ())
    |> Lwt_list.map_p
         (run_client ~host ~port ~request_quantity ~data_size ~key_space_size
            ~command_type ~request_counter ~request_time_buckets)
  in
  let end_ts = Unix.gettimeofday () in
  let fmt = Printf.sprintf in
  let open Lwt_io in
  let cmd_name =
    match command_type with
    | Set -> "SET"
    | Get -> "GET"
  in
  let number_of_requests = float_of_int !request_counter in
  let%lwt () = write_line stdout (fmt "====== %s ======" cmd_name) in
  let throughput = number_of_requests /. (end_ts -. start_ts) in
  let%lwt () = write_line stdout (fmt "%.2f requests per second " throughput) in
  let%lwt _ =
    Lwt_list.fold_left_s
      (fun total (bucket_value, quantity) ->
        let total = total + !quantity in
        let percentage = float_of_int total /. number_of_requests *. 100. in
        let%lwt () =
          write_line stdout
            (fmt "%.2f%% <= %i milliseconds " percentage bucket_value)
        in
        Lwt.return total)
      0
      (TimeBuckets.bindings request_time_buckets)
  in
  write_line stdout ""

let () =
  let open Arg in
  let host = ref "localhost" in
  let port = ref "14777" in
  let requests = ref 1000 in
  let clients = ref 50 in
  let data_size = ref 10 in
  let key_space_size = ref 1000 in
  let specs =
    [ ("-h", Set_string host, "Server host. Default: " ^ !host);
      ("-p", Set_string port, "Server port. Default: " ^ !port);
      ( "-n",
        Set_int requests,
        "Total number of requests. Default: " ^ string_of_int !requests );
      ( "-c",
        Set_int clients,
        "Number of parallel connections. Default: " ^ string_of_int !clients );
      ( "-d",
        Set_int data_size,
        "Data size of SET/GET value in bytes. Default: "
        ^ string_of_int !data_size );
      ( "-k",
        Set_int key_space_size,
        "Number of unique keys generated for SET/GET. Default: "
        ^ string_of_int !key_space_size ) ]
  in
  let usage = "flask-benchmark [options]" in
  parse specs (fun arg -> raise @@ Bad ("Unexpected argument: " ^ arg)) usage;
  let benchmark =
    run_benchmark ~client_quantity:!clients ~host:!host ~port:!port
      ~request_quantity:!requests ~data_size:!data_size
      ~key_space_size:!key_space_size
  in
  let benchmarks =
    let%lwt () = benchmark Set in
    benchmark Get
  in
  Lwt_main.run benchmarks
