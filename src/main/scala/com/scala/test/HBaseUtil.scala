package com.scala.test
import java.security.MessageDigest

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class HBaseUtil(spark:SparkSession) extends Serializable{

  /**
    * Spark 读写 HBase 有4种方式
    *  1. Java Api
    *  2. saveAsNewAPIHadoopDataset 使用Job
    *  3  saveAsHadoopDataset    使用JobConf
    *  4. BulkLoad (这里是使用开源插件)
    *
    *  在视频里演示的是第3种(旧API)方式
    *  在注释里会演示第2种(新API)方式
    *
    *  但效率最高的应该是第4种
    *
    *  这个类应该独立出来，因为在其他项目都会用到这个公共类
    *  这里并没有封装的很严谨
    *
    */

  @transient val hbaseConfig = HBaseConfiguration.create()
  //hbaseConfig.set("hbase.rootdir", "hdfs:\\\master:8020\\hbase")
  hbaseConfig.set("hbase.zookeeper.quorum", "master,worker1,worker2") // 设置zookeeper节点
  hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
   @transient val sc = spark.sparkContext

  //读取数据
  def getData(tableName:String,
              cf:String,
              column:String):DataFrame={

    hbaseConfig.set(TableInputFormat.INPUT_TABLE,
      tableName)
    val hbaseRDD:RDD[(ImmutableBytesWritable,Result)]
    = sc.newAPIHadoopRDD(hbaseConfig,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._
    val rs = hbaseRDD.map(_._2)
      .map(r=>{
        (r.getValue(
          Bytes.toBytes(cf),
          Bytes.toBytes(column)
        ))
      })
      .toDF("value")

    rs

  }

  //写入数据
  def putData(tableName:String,
              data:DataFrame,
              cf:String,
              column:String
             ):Unit={

    //初始化Job,设置输出格式TableOutputFormat，hbase.mapred.jar
     /* @transient val jobConf = new JobConf(hbaseConfig,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,
      tableName)*/

      // 使用新API
        val jobConf = new JobConf(hbaseConfig, this.getClass)
       jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
        val job =Job.getInstance(jobConf)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])




    val _data = data.rdd.map(x=> {
      val uid = x.getInt(0).toString
      val itemList = x.get(1)
      //在视频里没讲到，应该将rowKey散列
      //val rowKey = rowKeyHash(uid.toString)
      val rowKey = uid
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(cf),
        Bytes.toBytes(column),
        Bytes.toBytes(itemList.toString))
      (new ImmutableBytesWritable, put)
    })
    //_data.saveAsHadoopDataset(jobConf)
    _data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    /*
    新API
    _data.saveAsNewAPIHadoopDataset(job.getConfiguration)
   */
  }

  //rowKey散列
  def rowKeyHash(key:String):String={
    var md5:MessageDigest = null
    try {
      md5 = MessageDigest.getInstance("MD5")
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }
    //rowKey的组成：时间戳+uid
    val str = System.currentTimeMillis() + ":" + key
    val encode = md5.digest(str.getBytes())
    encode.map("%02x".format(_)).mkString
  }


}

object HBaseUtil extends Serializable {
  def apply(spark:SparkSession): HBaseUtil = new HBaseUtil(spark)
}
