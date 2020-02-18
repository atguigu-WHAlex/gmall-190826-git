package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestKafka")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP_TOPIC))

    //定义时间格式化 对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将每一行数据转换为样例类对象
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map { case (_, value) =>
      //将数据转换为样例类对象
      val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //取出时间戳
      val ts: Long = log.ts
      //将时间戳转换为字符串
      val logDateHour: String = sdf.format(new Date(ts))
      //截取字段
      val logDateHourArr: Array[String] = logDateHour.split(" ")
      //赋值日期&小时
      log.logDate = logDateHourArr(0)
      log.logHour = logDateHourArr(1)

      log
    }

    startLogDStream.print()

    //5.跨批次去重

    //6.同批次去重

    //7.将数据保存至Redis,以供下一次去重使用

    //8.有效数据(不做计算)写入HBase


    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}