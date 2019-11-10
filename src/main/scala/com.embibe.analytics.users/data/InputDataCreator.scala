package com.embibe.analytics.users.data

import java.sql.Timestamp

import com.embibe.analytics.users.configuration.UsersSessionGlobalConfiguration
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SaveMode, SQLContext}

class InputDataCreator(usersSessionGlobalConfiguration: UsersSessionGlobalConfiguration) {



  def generateRandomUserEventData(sqlContext: SQLContext): Unit = {

    val randEventList = List.range(1, 1000)

    val rowList = randEventList.map{ _ =>
      val offset = Timestamp.valueOf("2019-06-01 00:00:00").getTime
      val end = Timestamp.valueOf("2019-06-31 00:00:00").getTime
      val diff = end - offset + 1
      val randEvent = new Timestamp(offset + (Math.random * diff).toLong)
      println(randEvent)
      val rnd = new scala.util.Random
      val user =  1 + rnd.nextInt(5)
      Row.fromSeq(Seq(s"u$user", randEvent))
    }
    val inputRowSchema = StructType(
      Array(
        StructField("user", StringType),
        StructField("event_timestamp", TimestampType)
      )
    )

    val sc = sqlContext.sparkContext
    val inputDF = sqlContext.createDataFrame(sc.parallelize(rowList), inputRowSchema)

    val inputConfig = usersSessionGlobalConfiguration.input
    val filePath = inputConfig.getString("path")
    val resourceFile = if (inputConfig.hasPath("resource")) {
      inputConfig.getBoolean("resource")
    } else {
      false
    }
    val updatedFilePath = if (resourceFile) {
      getClass.getResource(filePath).getFile
    } else {
      filePath
    }

    inputDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save(updatedFilePath)
  }

}
