package io.github.holydrug.config

class YtClientPropertiesImpl(
  override val cluster: String,
  override val migration: Boolean,
  override val root: String,
  override val token: String,
  override val user: String,
  override val validation: Boolean
) : YtClientProperties