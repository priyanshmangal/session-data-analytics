package com.embibe.analytics.users.configuration

import com.typesafe.config.{Config, ConfigFactory}


class UsersSessionGlobalConfiguration(config: Config) extends Serializable {

  val input: Config = config.getConfig("input")
  val output: Config = config.getConfig("output")
}

object UsersSessionGlobalConfiguration {
  def apply(source: String): UsersSessionGlobalConfiguration = {
    val analyticsConfig = ConfigFactory.load(source)
    new UsersSessionGlobalConfiguration(analyticsConfig)
  }
}


