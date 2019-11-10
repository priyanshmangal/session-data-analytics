package com.embibe.analytics.users.executor

import java.sql.Timestamp

import com.embibe.analytics.users.configuration.UsersSessionGlobalConfiguration
import com.embibe.analytics.users.utility.sharedcontext.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class UsersUsageCohortJobTest extends FunSuite with SharedSparkContext {

  val applicationConfig = ConfigFactory.parseResources("analytics.conf")
  val usersSessionGlobalConfiguration = new UsersSessionGlobalConfiguration(applicationConfig)

  test("UsersUsageCohortJobTest - getSessionRow") {

    val rowList = List(
      Row("u1", Timestamp.valueOf("2019-03-01 10:00:00")),
      Row("u1", Timestamp.valueOf("2019-03-01 14:00:00"))
    )

    val inputRowSchema = StructType(
      Array(
        StructField("user", StringType),
        StructField("event_timestamp", TimestampType)
      )
    )

    val actualRowList = new UsersUsageCohortJob(usersSessionGlobalConfiguration).getSessionRow(rowList, inputRowSchema)

    val expectedRowList = List(
      Row("u1", Timestamp.valueOf("2019-03-01 10:00:00"), Timestamp.valueOf("2019-03-01 14:00:00"), 4)
    )

    assert(actualRowList === expectedRowList)
  }


  test("UsersUsageCohortJobTest - Jun Job") {


    UsersUsageCohortJob(usersSessionGlobalConfiguration).runJob(sqlContext)


  }

}
