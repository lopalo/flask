module FileNames = Map.Make (Int)
open Lwt.Infix

type ordered_file =
  { file_name : string;
    number : int;
    fd : Lwt_unix.file_descr;
    output_channel : Lwt_io.output_channel }

let find_ordered_file_names file_extension directory =
  Lwt_stream.fold
    (fun name names ->
      match String.split_on_char '.' name with
      | [number; ext] when ext = file_extension ->
          FileNames.add (int_of_string number)
            (Filename.concat directory name)
            names
      | _ -> names)
    (Lwt_unix.files_of_directory directory)
    FileNames.empty

let next_file_number file_names =
  match FileNames.max_binding_opt file_names with
  | None -> 0
  | Some (num, _) -> succ num

let next_file_name file_extension directory =
  find_ordered_file_names file_extension directory
  >|= next_file_number
  >|= fun number ->
  ( Filename.concat directory @@ string_of_int number ^ "." ^ file_extension,
    number )

let open_output_file ~replace file_name =
  let flags =
    [ Unix.O_WRONLY;
      Unix.O_CREAT;
      (if replace then Unix.O_TRUNC else Unix.O_EXCL);
      Unix.O_NONBLOCK ]
  in
  Lwt_unix.openfile file_name flags 0o666
  >>= fun fd -> Lwt.return (fd, Lwt_io.(of_fd ~mode:output fd))

let open_next_output_file ~replace file_extension directory =
  next_file_name file_extension directory
  >>= fun (file_name, number) ->
  open_output_file ~replace file_name
  >>= fun (fd, output_channel) ->
  Lwt.return {fd; output_channel; number; file_name}

let rec remove_dir path =
  let open Lwt_unix in
  if%lwt file_exists path then
    let%lwt s = stat path in
    match s.st_kind with
    | S_DIR ->
        let%lwt _ =
          files_of_directory path
          |> Lwt_stream.iter_s (fun name ->
                 if name.[0] <> '.' then remove_dir (Filename.concat path name)
                 else Lwt.return_unit)
        in
        rmdir path
    | S_REG -> unlink path
    | _ -> Lwt.return_unit
  else Lwt.return_unit
