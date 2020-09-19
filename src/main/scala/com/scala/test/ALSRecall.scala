package com.scala.test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

import scala.collection.mutable.ArrayBuffer


class ALSRecall(data:DataFrame) extends Serializable {

  //计算不同参数的模型
  def getModel(maxIter:Int,
               rankArray:Array[Int],
               regArray:Array[Double],
               alphaArray:Array[Double]):ALSModel= {

    val Array(training, test) =
      data.randomSplit(Array(0.8, 0.2))

    var mapModel = Map[Double,ALSModel]()
    val listMSE = ArrayBuffer[Double]()

    //这一步会有点耗时
    /**
      * 百万级数据
      * 以rankArray,regArray,alphaArray有2-3个候选值为例，耗时约15m
      *
      */
    for(rank <- rankArray;
        reg <- regArray;
        alpha <- alphaArray) {
      val als = new ALS()
        .setMaxIter(maxIter)
        .setUserCol("uid")
        .setItemCol("itemid")
        .setRatingCol("rating")
        .setRank(rank)
        .setRegParam(reg)
        .setImplicitPrefs(true) //真正项目要区分开显式和隐式
        .setAlpha(alpha)

      val model = als.fit(training)
      //冷启动的处理
      model.setColdStartStrategy("drop")
      //val predict = model.transform(test)
      //视频里的评估指标是mse
      /*   表结构  uid itemid rating prediction*/
      //这里只比较了rmse
//      val rmse = getEvaluate(predict)
//      //      println(rmse)
//      listMSE += rmse
//      mapModel += (rmse->model)

    }

    //获取最优的模型

    val minMSE = listMSE.min
    val bestModel = mapModel(minMSE)

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.format(new Date())
    val modelPath = "/model/als_model/"+date


    //把模型存入HDFS
    //存放als模型是因为之后的项目会用到这个模型
    //按日期存放
    bestModel.write.overwrite().save(modelPath)
    //bestModel.save(modelPath)

    bestModel

  }

  //获取模型的评估
  def getEvaluate(predict:DataFrame):Double= {

    val re = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = re.evaluate(predict)
    rmse
  }

  //获取als召回
  def getALSRecall(model:ALSModel,
                   spark:SparkSession):DataFrame={
    val list = model.recommendForAllUsers(10)

    /**
      *  list表结构
      *  uid  recommendations
      *  2    [[34,0.782],[56,0.94224],[78,0.4294]]
      *
      * uid  recommend
      *  2   [34,0.782]
      *  2   [56,0.94224]
      *  2   [78,0.4294]
      *
      *  目标生成的表结构
      *   uid  itemid
      * *  2     34
      * *  2     56
      * *  2     78
      *
      */

    import spark.implicits._
    val recallData = list.withColumn("recommend",
      explode(col("recommendations")))
      .drop("recommendations")
      .select("uid","recommend")
      .rdd.map(row=>{
      val uid = row.getInt(0)
      val recommend = row.getStruct(1)
      val itemid = recommend.getAs[Int]("itemid")
      (uid,itemid)
    }).toDF("uid","itemid")

    recallData
  }
}

object ALSRecall{
  def apply(data: DataFrame):ALSRecall = new ALSRecall(data)
}

