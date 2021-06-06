variable "account_id" { type = string }
variable "prefix" { type = string }
variable "environment" {
  type = string
}
variable "tags" {
  type = map(string)
}