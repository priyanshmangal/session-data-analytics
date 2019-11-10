package com.embibe.analytics.users.executor

import com.embibe.analytics.users.configuration.UsersSessionGlobalConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext



object UsersCohortUsageSparkJob {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("session_usage_cohort")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val usersSessionGlobalConfiguration = UsersSessionGlobalConfiguration("analytics")

    UsersUsageCohortJob(usersSessionGlobalConfiguration).runJob(sqlContext)
  }

}
