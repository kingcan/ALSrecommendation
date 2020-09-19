package com.scala.test
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, SparkSession}

object Recall {
  //column
  val colALS = "als"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[Configuration]))
    //conf.registerKryoClasses(Array(classOf[HBaseConfiguration]))
    val spark = SparkSession.builder()
      .config(conf)
      .appName("Recall")
      .master("yarn-cluster")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /**
      *  通常这个任务运行时间是在凌晨0-3am
      *  执行的数据是前一天的数据
      *
      *  例如现在是周三凌晨运行这两个召回任务
      *  使用的样本数据是周二产生的用户行为数据
      *
      *  一般每个召回策略各自独立项目分开运行
      *
      */


    //数据处理
    val modelUtil = ModelUtil(spark)
    val data:DataFrame = modelUtil.getUserItemRating

    //生成候选集


    //召回1：ALS
    val als = ALSRecall(data)
    //迭代次数 ，一般 10 左右
    val maxIter = 10

    //正则，不要太大
    val reg = Array(0.05,0.005)
    //维度数目，一般越多越好，计算时间也会越长，
    //一般在 10 - 200
    val rank = Array(20,80)
    //学习率，不要太大
    val alpha = Array(2.0,3.0)


    //生成最佳的ALS模型
    val model:ALSModel = als.getModel(maxIter,rank,reg,alpha)
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2))
    val alsTestRecallData = model.transform(test)
    val alsRecallData = als.getALSRecall(model,spark)
     println("训练完成")
    val rmse = getEvaluate(predict)
    //      println(rmse)
    listMSE += rmse
    mapModel += (rmse->model)
    //存储候选集
    modelUtil.saveRecall(alsTestRecallData,colALS)
    //modelUtil.saveRecall(alsRecallData,colALS)

    spark.stop()
  }
}

