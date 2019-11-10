# Usage Cohort
# Embibe

# session Creation


#Cohort Category:
AU (Active Users): Any user who was active at some point of time on Embibe platform

EU (Engaged Users): users with >= 5 hours of overall timespent till date (users may have < 40 sessions, but total timespent >= 5 hour)

PU (Power Users): users with >= 40 sessions on Embibe.com in last 30 days

UPU (Ultra Power Users): users with >= 100 sessions in Embibe.com in last 30 days	


# Spark Submit Command 
spark-submit --master yarn-cluster--executor-memory 15G --num-executors 7 --executor-cores 3
 --driver-java-options "-Duser.timezone=UTC" --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC"
  --class com.embibe.analytics.users.executor.UsersCohortUsageSparkJob "jar path"