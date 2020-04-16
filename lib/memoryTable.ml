module H = Hashtbl
open Common

module Storage = struct
  type t =
    { mutable keys : keys;
      mutable deleted_keys : keys;
      values : values }

  let create ?(size = 4096) () =
    {keys = Keys.empty; deleted_keys = Keys.empty; values = H.create size}

  let size {values; _} = H.length values

  let set_value ({values; _} as storage) key value =
    let old_value = H.find_opt values key in
    H.replace values key value;
    let keys =
      let keys = storage.keys in
      match (old_value, value) with
      | None, Value _
      | Some Nothing, Value _ ->
          Keys.add key keys
      | Some (Value _), Nothing -> Keys.remove key keys
      | Some (Value _), Value _
      | Some Nothing, Nothing
      | None, Nothing ->
          keys
    in
    let deleted_keys =
      let deleted_keys = storage.deleted_keys in
      match (old_value, value) with
      | None, Nothing
      | Some (Value _), Nothing ->
          Keys.add key deleted_keys
      | Some Nothing, Value _ -> Keys.remove key deleted_keys
      | Some Nothing, Nothing
      | Some (Value _), Value _
      | None, Value _ ->
          deleted_keys
    in
    storage.keys <- keys;
    storage.deleted_keys <- deleted_keys

  let get_value {values; _} key = H.find_opt values key

  let key_range keys start_key end_key =
    let _, start_present, keys = Keys.split start_key keys in
    let keys = if start_present then Keys.add start_key keys else keys in
    let keys, _, _ = Keys.split end_key keys in
    keys

  let search_key_range {keys; deleted_keys; _} start_key end_key result =
    let keys = key_range keys start_key end_key in
    let deleted_keys = key_range deleted_keys start_key end_key in
    Keys.diff result deleted_keys |> Keys.union keys
end

type t =
  { mutable storage : Storage.t;
    (* Must be read-only *)
    mutable previous_storage : Storage.t option }

let create ?size () =
  {storage = Storage.create ?size (); previous_storage = None}

let set_value {storage; _} key value =
  match Storage.get_value storage key with
  | Some value' when value' = value -> false
  | Some _
  | None ->
      Storage.set_value storage key value;
      true

let get_value {storage; previous_storage} key =
  match Storage.get_value storage key with
  | Some _ as v -> v
  | None -> (
    match previous_storage with
    | Some s -> Storage.get_value s key
    | None -> None)

let search_key_range {storage; previous_storage} start_key end_key result =
  let keys =
    match previous_storage with
    | Some s -> Storage.search_key_range s start_key end_key result
    | None -> result
  in
  Storage.search_key_range storage start_key end_key keys

let start_flushing memtable =
  let s = memtable.storage in
  memtable.storage <- Storage.(create ~size:(size s) ());
  memtable.previous_storage <- Some s;
  s.values

let end_flushing memtable = memtable.previous_storage <- None
