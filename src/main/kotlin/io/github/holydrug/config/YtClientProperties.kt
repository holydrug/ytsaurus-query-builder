package io.github.holydrug.config

interface YtClientProperties {
  val cluster: String
  val user: String
  val token: String
  val root: String
  val migration: Boolean
  val validation: Boolean
}