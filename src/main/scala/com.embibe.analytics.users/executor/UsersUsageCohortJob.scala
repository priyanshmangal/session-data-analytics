package com.embibe.analytics.users.executor

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import com.embibe.analytics.users.configuration.UsersSessionGlobalConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, Hours, Minutes}
import org.slf4j.{Logger, LoggerFactory}



/** UsersUsageCohortJob
  *
  */

class UsersUsageCohortJob(usersSessionGlobalConfiguration: UsersSessionGlobalConfiguration) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy private val jobExecutionTime: String = s"exec-${System.currentTimeMillis()}"

  logger.info("Spark Job Run System Time -" + jobExecutionTime)

  def runJob(sqlContext: SQLContext): Unit = {
    execute(sqlContext)
  }

  /** Execute
    * Based On randomly generated data
    * calculated:
    * 1) Session of user based on event
    * 2) day to day and weekly visualisation of user cohort group status.
    *
    * @param sqlContext   : sqlContext
    */
  def execute(sqlContext: SQLContext): Unit = {

    val inputDF = readInputData(sqlContext)
    val sessionOutputRDD = getSessionInformation(inputDF)

    val outputRowSchema = StructType(
      Array(
        StructField("user", StringType),
        StructField("session_start_timestamp", TimestampType),
        StructField("session_end_timestamp", TimestampType),
        StructField("session_active_time", IntegerType),
        StructField("cohort_status", StringType)
      )
    )

    val sessionOutputDF = sqlContext.createDataFrame(sessionOutputRDD, outputRowSchema)


    val dayByDayVisualiseRDD = getDayByDayVisualiseOutput(sessionOutputRDD, outputRowSchema)

    val dayByDayVisualiseSchema = StructType(
      Array(
        StructField("date", DateType),
        StructField("active_user", IntegerType),
        StructField("engaged_user", IntegerType),
        StructField("power_user", IntegerType),
        StructField("ultra_power_user", IntegerType)
      )
    )

    val dayByDayVisualiseOutputDF = sqlContext.createDataFrame(dayByDayVisualiseRDD, dayByDayVisualiseSchema)

    val weeklyVisualiseRDD = getWeeklyVisualiseOutput(sessionOutputRDD, outputRowSchema)

    val weeklyVisualiseSchema = StructType(
      Array(
        StructField("month", IntegerType),
        StructField("active_user", IntegerType),
        StructField("engaged_user", IntegerType),
        StructField("power_user", IntegerType),
        StructField("ultra_power_user", IntegerType)
      )
    )

    val weeklyVisualiseOutputDF = sqlContext.createDataFrame(weeklyVisualiseRDD, weeklyVisualiseSchema)

    val outputConfig = usersSessionGlobalConfiguration.output
    val filePath = outputConfig.getString("path")

    writeOutputDataFrame(sessionOutputDF, filePath+ "/sessionInformation")
    writeOutputDataFrame(dayByDayVisualiseOutputDF, filePath+ "/dayByDayVisualisation")
    writeOutputDataFrame(weeklyVisualiseOutputDF, filePath+ "/weeklyVisualisation")
  }

  /** getDayByDayVisualiseOutput
    * On Day to Day basis analysis user distribution in each category
    * On Each Day consider cohort status of last end session timestamp
    *
    * @param sessionRDD
    * @param sessionSchema
    * @return RDD of row
    */
  def getDayByDayVisualiseOutput(sessionRDD : RDD[Row], sessionSchema: StructType): RDD[Row] = {

    val sessionStartTimeStampIndex = sessionSchema.fieldIndex("session_start_timestamp")
    val sessionEndTimeStampIndex = sessionSchema.fieldIndex("session_end_timestamp")
    val cohortStatusIndex = sessionSchema.fieldIndex("cohort_status")
    val visualiseAnalysisGroupData = sessionRDD.groupBy { row =>
        val formatter = new SimpleDateFormat("dd/MM/yyyy")
        val date = new Date(row.getTimestamp(sessionStartTimeStampIndex).getTime)
        val dateWithZeroTime = formatter.parse(formatter.format(date))
        dateWithZeroTime
    }.sortBy(_._1)

    visualiseAnalysisGroupData.map { case (date, rowList) =>
        var au = 0
        var eu = 0
        var pu = 0
        var upu = 0
        rowList.groupBy(row => row.getString(0)).foreach{case (user, userRowList) =>
          val cohortStatus = userRowList.toList.maxBy(row => row.getTimestamp(sessionEndTimeStampIndex).getTime)
            .getString(cohortStatusIndex)

          cohortStatus match {
            case "AU" => au+=1
            case "EU" => eu+=1
            case "PU" => pu+=1
            case "UPU" => upu+=1
            case _ => throw new IllegalArgumentException("wrong cohort category")
          }
        }
      val sessionDate = new java.sql.Date(date.getTime)
      Row.fromSeq(Seq(sessionDate, au, eu, pu, upu))
    }
  }


  /** Weekly Visualise Distribution of user in each category.
    * User Last session of week will be considered as cohort status
    *
    * @param sessionRDD
    * @param sessionSchema
    * @return
    */
  def getWeeklyVisualiseOutput(sessionRDD : RDD[Row], sessionSchema: StructType): RDD[Row] = {

    val sessionStartTimeStampIndex = sessionSchema.fieldIndex("session_start_timestamp")
    val sessionEndTimeStampIndex = sessionSchema.fieldIndex("session_end_timestamp")
    val cohortStatusIndex = sessionSchema.fieldIndex("cohort_status")
    val visualiseAnalysisGroupData = sessionRDD.groupBy { row =>
      new DateTime(row.getTimestamp(sessionStartTimeStampIndex).getTime).getWeekOfWeekyear
    }.sortBy(_._1)

    visualiseAnalysisGroupData.map { case (week, rowList) =>
      var au = 0
      var eu = 0
      var pu = 0
      var upu = 0
      rowList.groupBy(row => row.getString(0)).foreach{case (user, userRowList) =>
        val cohortStatus = userRowList.toList.maxBy(row => row.getTimestamp(sessionEndTimeStampIndex).getTime)
            .getString(cohortStatusIndex)

        cohortStatus match {
          case "AU" => au+=1
          case "EU" => eu+=1
          case "PU" => pu+=1
          case "UPU" => upu+=1
          case _ => throw new IllegalArgumentException("wrong cohort category")
        }
      }
      Row.fromSeq(Seq(week, au, eu, pu, upu))
    }
  }


  /** getSessionInformation
    * For each user on each day get session for user.
    * i.e. user event is within 5hours of that is considered as one session.
    * Classify session of user in variuous cohort category based on total active session time and session
    * count within month
    *
    * @param inputDF
    * @return
    */
  def getSessionInformation(inputDF: DataFrame): RDD[Row] = {
    val inputSchema = inputDF.schema

    val userColumnIndex = inputSchema.fieldIndex("user")
    val timestampIndex = inputSchema.fieldIndex("event_timestamp")

    val userGroupedInputRDD = inputDF.rdd.groupBy { row =>
      val user = row.getString(userColumnIndex)
      user
    }

    val sessionRowRDD = userGroupedInputRDD.flatMap {
      case (user, rowList) =>
        val dateGroupRowList = rowList.groupBy { row =>
          val formatter = new SimpleDateFormat("dd/MM/yyyy")
          val date = new Date(row.getTimestamp(timestampIndex).getTime)
          val dateWithZeroTime = formatter.parse(formatter.format(date))
          dateWithZeroTime
        }.toList.sortBy(_._1)

        val sessionRowList = dateGroupRowList.flatMap{
          case (date, dateRowList) =>
          getSessionRow(dateRowList.toList, inputSchema)
        }

        var activeHours = 0
        var sessionCount = 0
        val cohortStatusRowList = sessionRowList.map{ sessionRow =>
          activeHours = activeHours + sessionRow.getInt(3)
          sessionCount = sessionCount + 1
          val cohortStatus = if ( sessionCount >= 100){
            "UPU"
          } else if(sessionCount >= 40) {
            "PU"
          } else if (activeHours >= 5) {
            "EU"
          } else {
            "AU"
          }
          Row.fromSeq(sessionRow.toSeq ++ Seq(cohortStatus))
        }
        cohortStatusRowList
    }

    sessionRowRDD
  }

  /** getSessionRow
    * Generating Session Start and End Time based on event row list
    * All event within 5 hours are considerd as one session
    *
    * @param rowList
    * @param inputSchema
    * @return
    */
  def getSessionRow(rowList: List[Row], inputSchema: StructType): List[Row] = {

    val timestampIndex = inputSchema.fieldIndex("event_timestamp")
    val userIndex = inputSchema.fieldIndex("user")
    val sortedRowList = rowList.sortBy(x => x.getTimestamp(timestampIndex).getTime)
    val minimumTimeStamp = sortedRowList.head.getTimestamp(timestampIndex)
    val maximumTimeStamp = sortedRowList.last.getTimestamp(timestampIndex)

    val user = rowList.head.getString(userIndex)
    var sessionStartDateTime = new DateTime(minimumTimeStamp)
    var sessionEndDateTime = new DateTime(maximumTimeStamp)

    val updatedSessionRowList = sortedRowList.flatMap { row =>
      val firstRowTime = new DateTime(row.getTimestamp(timestampIndex))
      val hoursDifference = Hours.hoursBetween(sessionStartDateTime, firstRowTime).getHours()
      if ( hoursDifference >= 5) {
        val finalSessionStartTime = sessionStartDateTime
        val finalSessionEndTime = sessionEndDateTime

        sessionStartDateTime = firstRowTime
        sessionEndDateTime = firstRowTime

        val sessionHour = Hours.hoursBetween(finalSessionStartTime, finalSessionEndTime).getHours()
        val row = Row.fromSeq(Seq(user, new Timestamp(finalSessionStartTime.getMillis),
          new Timestamp(finalSessionEndTime.getMillis), sessionHour))
        if (firstRowTime == new DateTime(maximumTimeStamp) ) {
          val lastSessionHour = Hours.hoursBetween(sessionStartDateTime, firstRowTime).getHours()
          val lastRow = Row.fromSeq(Seq(user, new Timestamp(sessionStartDateTime.getMillis),
            new Timestamp(firstRowTime.getMillis), lastSessionHour))
          List(row, lastRow)
        } else {
          List(row)
        }
      } else if (firstRowTime == new DateTime(maximumTimeStamp) ) {
        val sessionHour = Hours.hoursBetween(sessionStartDateTime, firstRowTime).getHours()
        val row = Row.fromSeq(Seq(user, new Timestamp(sessionStartDateTime.getMillis),
          new Timestamp(firstRowTime.getMillis), sessionHour))
        List(row)
      }
      else {
        sessionEndDateTime = firstRowTime
        List.empty
      }
    }
    updatedSessionRowList
  }

  /** writeOutputDataFrame
    *
    * @param outputDF
    * @param filePath
    */
  def writeOutputDataFrame(outputDF: DataFrame, filePath: String): Unit = {
    outputDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save(filePath)
  }

  /** readInputData : Read Input Dataframe
    *
    * @param sqlContext
    * @return
    */
  def readInputData(sqlContext: SQLContext): DataFrame = {

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
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(updatedFilePath)

    readDF
  }

}

object UsersUsageCohortJob {
  def apply(usersSessionGlobalConfiguration:
            UsersSessionGlobalConfiguration): UsersUsageCohortJob
  = new UsersUsageCohortJob(usersSessionGlobalConfiguration)
}
