package com.scala.test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object ViewScore {
  def main(args: Array[String]): Unit = {
     println("第一个参数："+args(0)+"为当前时间的前N天日期")
    /**
      * 定时任务
      * （单独对每一个用户行为进行计算，也可以合并一起计算）
      *  用户行为数值 * 时间权重
      *
      */

    //点击详情页权重
    val clickW = 0.22

    //点击详情页分值
    val click = 1.0


    val spark = SparkSession.builder()
      .master("yarn-cluster")
      .enableHiveSupport()
      .getOrCreate()

    //获取年月日
//    val calendar = Calendar.getInstance()
//    val year = calendar.get(Calendar.YEAR)
//    val month = calendar.get(Calendar.MONTH)+1
//    val day = calendar.get(Calendar.DATE)
//    val viewScore = spark.sql(
//      "select uid," +
//        "item_id," +
//        "sum(view)* "+click+"*"+clickW+"* interval" +
//        "as view_score" +
//        "from dwd.dwd_behavior_view " +
//        "where type = 'detail_info' " +
//        "and year='"+year+"' " +
//        "and month='"+month+"' " +
//        "and day='"+day+"' "+
//        "group by uid,item_id")
//
//
//    viewScore.createOrReplaceTempView("view_score")
//    //构建每天的打分表
//    spark.sql("insert overwrite table " +
//      "dws.dws_behavior_score select *"+
//      year +" as year" +
//      month +" as month" +
//      day +" as day from view_score")

    ////此处是获取五月1号的日期,我们演示版的代码只负责简单化hive表
    val calendar1 = Calendar.getInstance
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    calendar1.add(Calendar.DATE, args(0).toInt)
    val N_days_ago = sdf1.format(calendar1.getTime)

    val viewScore = spark.sql(
      "select c.member_id," +
        "c.id," +
        "count(c.create_time)," +
        "sum(c.plytotal) "+
        "from " +
        "(select a.member_id as member_id,"+
        "b.id as id," +
        "b.asset_id as asset_id,"+
        "a.create_time as create_time,"+
        "a.play_duration as plytotal" +
        "from testonly.dms_collection_vod_play as a" +
        "left join" +
        "testonly.bo_video_content as b" +
        "on a.parent_asset_id=b.asset_id " +
        "where a.date_ymd='"+N_days_ago +"'"+
        ") as c " +
        "group by member_id,id")

    viewScore.createOrReplaceTempView("view_score")
    //构建每天的打分表
    spark.sql("insert overwrite table " +
      "testonly.dms_user_behavior_crude select *"+
      " from view_score")


    val viewScore2 = spark.sql(
      "select member_id," +
        "item_id," +
        "play_times+(plytotal/60)" +
        "as view_score2" +
        "from testonly.dms_user_behavior_crude " )

    viewScore2.createOrReplaceTempView("view_score2")
    //构建每天的打分表
    spark.sql("insert overwrite table " +
      "testonly.dms_user_behavior select *"+
      " from view_score2")

    spark.stop()
  }


}

