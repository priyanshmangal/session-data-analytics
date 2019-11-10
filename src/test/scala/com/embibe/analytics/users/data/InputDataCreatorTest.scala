package com.embibe.analytics.users.data

import com.embibe.analytics.users.configuration.UsersSessionGlobalConfiguration
import com.embibe.analytics.users.utility.sharedcontext.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class InputDataCreatorTest extends FunSuite with SharedSparkContext {

  test("InputDataCreate - generate user event data") {

    val applicationConfig = ConfigFactory.parseResources("analytics.conf")
    val usersSessionGlobalConfiguration = new UsersSessionGlobalConfiguration(applicationConfig)

    new InputDataCreator(usersSessionGlobalConfiguration).generateRandomUserEventData(sqlContext)

  }

  test("dfdf") {

  }
}
