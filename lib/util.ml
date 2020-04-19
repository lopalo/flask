open Common

let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let seconds {milliseconds} = {seconds = Float.of_int milliseconds /. 1000.0}

let bytes {megabytes} = {bytes = megabytes * 1024 * 1024}

open Lwt.Infix

let write_uint8 oc i = Lwt_io.write_char oc (Char.unsafe_chr i)

let write_short_string oc str =
  write_uint8 oc (String.length str) >>= fun () -> Lwt_io.write oc str

let write_long_string oc str =
  String.length str |> Int32.of_int |> Lwt_io.BE.write_int32 oc
  >>= fun () -> Lwt_io.write oc str

let write_many_short_strings channel strings =
  List.length strings |> Int32.of_int
  |> Lwt_io.BE.write_int32 channel
  >>= fun () ->
  let rec loop = function
    | s :: strings -> write_short_string channel s >>= fun () -> loop strings
    | [] -> Lwt.return_unit
  in
  loop strings

module Parser = struct
  open Angstrom

  let short_string = any_uint8 >>= fun length -> take length

  let long_string = BE.any_int32 >>= fun length -> take (Int32.to_int length)

  let many_short_strings =
    BE.any_int32 >>= fun amount -> count (Int32.to_int amount) short_string
end

let run_periodically milliseconds f =
  let period = seconds milliseconds in
  let rec loop () =
    let sleep = Lwt_unix.sleep period.seconds in
    f () >>= fun () -> sleep >>= loop
  in
  loop ()
