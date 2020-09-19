package com.scala.test

import java.util.Calendar

import org.apache.spark.sql.SparkSession

object UserItemRating {
  def main(args: Array[String]): Unit = {

    /**
      *
      * 定时任务
      * 生成用户-物品评分表
      *
      */


    val spark = SparkSession.builder()
      .master("yarn-cluster")
      .enableHiveSupport()
      .getOrCreate()

    //获取年月日
    val calendar = Calendar.getInstance()
    val year = calendar.get(Calendar.YEAR)
    val month = calendar.get(Calendar.MONTH)+1
    val day = calendar.get(Calendar.DATE)

    //sigmoid
    spark.udf.register("scaler",
      (score:Double)=>2*1/(1+math.exp(-score)))

    val scaler = spark.sql("select member_id," +
      "item_id," +
      "scaler(score) as rating" +
      "from testonly.dms_user_behavior")

    scaler.createOrReplaceTempView("scaler")

    spark.sql("insert overwrite " +
      "table testonly.dms_user_item_rating" +
      " select * from scaler")
  }

}

