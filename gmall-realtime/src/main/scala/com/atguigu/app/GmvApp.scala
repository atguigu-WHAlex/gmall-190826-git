package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))

    //4.将每一行数据转换为样例类对象
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map { case (_, value) =>

      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      //处理创建日期及小时 2020-02-21 12:12:12
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      //手机号脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"

      //返回数据
      orderInfo
    }

    //5.将订单数据写入HBase(Phoenix)
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL190826_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
