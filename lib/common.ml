type key = Key of string [@@unboxed]

type value =
  | Removal
  | Value of string
