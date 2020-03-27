let rec forever () = Lwt.bind (Lwt_unix.sleep 60.) (fun _ -> forever ())

let seconds milliseconds = Float.of_int milliseconds /. 1000.0

open Lwt.Infix

let write_uint8 oc i = Lwt_io.write_char oc (Char.unsafe_chr i)

let write_short_string oc str =
  write_uint8 oc (String.length str) >>= fun () -> Lwt_io.write oc str

let write_long_string oc str =
  String.length str |> Int32.of_int |> Lwt_io.BE.write_int32 oc
  >>= fun () -> Lwt_io.write oc str

open Angstrom

let short_string_parser = any_uint8 >>= fun length -> take length

let long_string_parser =
  BE.any_int32 >>= fun length -> take (Int32.to_int length)
