type key = Key of string [@@unboxed]

type value =
  | Nothing
  | Value of string

type storage = (key, value) Hashtbl.t

type memory_table =
  { mutable storage : storage;
    (* Must be read-only *)
    mutable previous_storage : storage option }

let max_key_length = 255

let max_value_length = 2147483647

let is_valid_key_length (Key k) = String.length k <= max_key_length

let is_valid_value_length = function
  | Value v -> String.length v <= max_value_length
  | Nothing -> true
