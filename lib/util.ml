open Common

let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let seconds {milliseconds} = {seconds = Float.of_int milliseconds /. 1000.0}

let bytes {megabytes} = {bytes = int_of_float @@ (megabytes *. 1024.0 *. 1024.0)}

let megabytes {bytes} = {megabytes = Float.of_int bytes /. 1024.0 /. 1024.0}

open Lwt.Infix

let write_uint8 channel i = Lwt_io.write_char channel (Char.unsafe_chr i)

let write_short_string channel str =
  write_uint8 channel (String.length str) >>= fun () -> Lwt_io.write channel str

let write_long_string channel str =
  String.length str |> Int32.of_int
  |> Lwt_io.BE.write_int32 channel
  >>= fun () -> Lwt_io.write channel str

let write_many_short_strings channel strings =
  List.length strings |> Int32.of_int
  |> Lwt_io.BE.write_int32 channel
  >>= fun () ->
  let rec loop = function
    | s :: strings -> write_short_string channel s >>= fun () -> loop strings
    | [] -> Lwt.return_unit
  in
  loop strings

let read_uint8 channel = Lwt_io.read_char channel >|= Char.code

let read_exactly channel length =
  let buffer = Bytes.create length in
  Lwt_io.read_into_exactly channel buffer 0 length
  >|= fun () -> Bytes.unsafe_to_string buffer

let read_short_string channel = read_uint8 channel >>= read_exactly channel

let read_long_string channel =
  Lwt_io.BE.read_int32 channel >|= Int32.to_int >>= read_exactly channel

let read_n_times channel read_fun n =
  let rec f acc = function
    | 0 -> Lwt.return acc
    | n -> read_fun channel >>= fun x -> f (x :: acc) (pred n)
  in
  f [] n >|= List.rev

let read_many_short_strings channel =
  Lwt_io.BE.read_int32 channel
  >|= Int32.to_int
  >>= read_n_times channel read_short_string

let run_periodically milliseconds f =
  let period = seconds milliseconds in
  let rec loop () =
    let sleep = Lwt_unix.sleep period.seconds in
    f () >>= fun () -> sleep >>= loop
  in
  loop ()
