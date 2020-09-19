package com.scala.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ModelUtil(spark:SparkSession) extends Serializable {

  //当次召回推荐表
  val rsRecall = "history_rs_recall"
  //cf
  val cf = "recall"


  //获取样本数据
  def getUserItemRating: DataFrame ={
    val data = spark.sql(
      "select * from testonly.dms_user_item_rating where uid != -1")

    data

  }

  //把召回策略生成的推荐候选集存储到HBase
  def saveRecall(recommend:DataFrame,cell:String):Unit={

    /**
      * 目标生成格式
      *  uid     itemid
      *  3       [12,34,24,89,21]
      *  5       [19,78,67,21,12]
      */
      //LR中要反着过来
    val recommList = recommend.groupBy(col("uid"))
      .agg(collect_list("itemid"))
      .withColumnRenamed("collect_list(itemid)",
        "itemid")
      .select(col("uid"),
        col("itemid"))

    val HBase = new HBaseUtil(spark)
    HBase.putData(rsRecall,recommList,cf,cell)
  }


}

object ModelUtil extends Serializable {
  //var spark:SparkSession = _
  def apply(spark:SparkSession):ModelUtil = new ModelUtil(spark)
}

