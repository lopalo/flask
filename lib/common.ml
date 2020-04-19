type milliseconds = {milliseconds : int} [@@unboxed]

type seconds = {seconds : float} [@@unboxed]

type bytes = {bytes : int} [@@unboxed]

type megabytes = {megabytes : int} [@@unboxed]

type key = Key of string [@@unboxed]

module Key = struct
  type t = key

  let equal = ( = )

  let compare = compare

  let hash = Hashtbl.hash
end

module Keys = Set.Make (Key)

type keys = Keys.t

type value =
  | Nothing
  | Value of string

type values = (key, value) Hashtbl.t

let max_key_length = 255

let max_value_length = 2147483647

let is_valid_key_length (Key k) = String.length k <= max_key_length

let is_valid_value_length = function
  | Value v -> String.length v <= max_value_length
  | Nothing -> true

let max_value_weight = 100

let value_weight_fraction = max_value_length / max_value_weight

module Value = struct
  type t = value

  let weight = function
    | Nothing -> 1
    | Value v ->
        let weight = String.length v / value_weight_fraction in
        min max_value_weight (max 1 weight)
end
