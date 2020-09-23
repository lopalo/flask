module State = ServerState
open Lwt.Infix
open Common

type result =
  | Done of Protocol.cmd_result
  | WaitWriteSync of unit Lwt.t Lwt.t * Protocol.cmd_result
  | PullValues of key list
  | Wait of Protocol.cmd_result Lwt.t

let one_result value =
  Done
    (Ok
       (match value with
       | Nothing -> Protocol.Nil
       | Value v -> OneLong v))

let bool_value value = Ok (Protocol.OneLong (Bool.to_string value))

let error message = Done (Error (ResponseError message))

let pt_is_writing_error = error "Persistent table is writing files to disk"

let key_range_error = error "Start key is greater than end key"

let set_value state key value =
  match State.set_value state (Key key) (Value value) with
  | Some p -> WaitWriteSync (p, bool_value true)
  | None -> Done (bool_value false)

let set_and_return_value state key value =
  match State.set_value state (Key key) (Value value) with
  | Some p -> WaitWriteSync (p, Ok (OneLong value))
  | None -> Done (bool_value false)

let get_value = State.get_value

let handle state =
  let module P = Protocol in
  let module S = State in
  function
  | P.Set {key; value} -> set_value state key value
  | Get {key} -> (
      let k = Key key in
      match get_value state k with
      | Some value -> one_result value
      | None -> PullValues [k])
  | Delete {key} -> (
    match State.set_value state (Key key) Nothing with
    | Some p -> WaitWriteSync (p, bool_value true)
    | None -> Done (bool_value false))
  | Flush ->
      if S.is_persistent_table_writing state then pt_is_writing_error
      else
        Wait
          (State.flush_memory_table state
          >|= fun records_amount ->
          Ok (P.OneLong (string_of_int records_amount)))
  | Compact ->
      if S.is_persistent_table_writing state then pt_is_writing_error
      else
        Wait
          (S.compact_persisten_table state >|= fun status -> bool_value status)
  | Keys {start_key; end_key} ->
      if start_key > end_key then key_range_error
      else
        Wait
          (S.search_key_range state (Key start_key) (Key end_key)
          >|= fun keys ->
          Ok (P.ManyShort (Keys.elements keys |> List.map (fun (Key k) -> k))))
  | Count {start_key; end_key} ->
      if start_key > end_key then key_range_error
      else
        Wait
          (S.search_key_range state (Key start_key) (Key end_key)
          >|= fun keys -> Ok (P.OneLong (Keys.cardinal keys |> string_of_int)))
  | Add {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some (Value _) -> Done (bool_value false)
      | Some Nothing -> set_value state key value)
  | Replace {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some Nothing -> Done (bool_value false)
      | Some (Value _) -> set_value state key value)
  | CAS {key; old_value; new_value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some (Value current_value) when current_value <> old_value ->
          Done (bool_value false)
      | Some (Value _)
      | Some Nothing ->
          set_value state key new_value)
  | Swap {key1; key2} -> (
      let k1, k2 = (Key key1, Key key2) in
      match (get_value state k1, get_value state k2) with
      | None, None -> PullValues [k1; k2]
      | None, Some _ -> PullValues [k1]
      | Some _, None -> PullValues [k2]
      | Some v1, Some v2 -> (
        match S.set_values state [(k1, v2); (k2, v1)] with
        | Some p -> WaitWriteSync (p, bool_value true)
        | None -> Done (bool_value false)))
  | Append {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some v ->
          let new_value =
            match v with
            | Nothing -> value
            | Value value' -> value' ^ value
          in
          set_value state key new_value)
  | Prepend {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some v ->
          let new_value =
            match v with
            | Nothing -> value
            | Value value' -> value ^ value'
          in
          set_value state key new_value)
  | Incr {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some v -> (
        match v with
        | Nothing -> set_and_return_value state key (string_of_int value)
        | Value value' -> (
          match int_of_string_opt value' with
          | None -> error "Current value is not an integer"
          | Some i -> set_and_return_value state key (string_of_int (i + value))
          )))
  | Decr {key; value} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some v -> (
        match v with
        | Nothing -> set_and_return_value state key (string_of_int (-value))
        | Value value' -> (
          match int_of_string_opt value' with
          | None -> error "Current value is not an integer"
          | Some i -> set_and_return_value state key (string_of_int (i - value))
          )))
  | GetLength {key} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some value ->
          one_result
            (Value
               (match value with
               | Nothing -> "0"
               | Value v -> String.length v |> string_of_int)))
  | GetRange {key; start; length} -> (
      let k = Key key in
      match get_value state k with
      | None -> PullValues [k]
      | Some value -> (
        try
          one_result
            (Value
               (match value with
               | Nothing -> ""
               | Value v -> String.sub v start length))
        with Invalid_argument _ -> error "Invalid range"))
  | GetStats -> Wait (S.get_stats state >|= fun stats -> Ok (P.ManyShort stats))
