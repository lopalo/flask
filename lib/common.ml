type milliseconds = {milliseconds : int} [@@unboxed]

type seconds = {seconds : float} [@@unboxed]

type bytes = {bytes : int} [@@unboxed]

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

let max_value_weight = 100

let value_weight_fraction = max_value_length / max_value_weight

module Key = struct
  type t = key

  let equal = ( = )

  let hash = Hashtbl.hash
end

module Value = struct
  type t = value

  let weight = function
    | Nothing -> 1
    | Value v ->
        let weight = String.length v / value_weight_fraction in
        min max_value_weight (max 1 weight)
end
