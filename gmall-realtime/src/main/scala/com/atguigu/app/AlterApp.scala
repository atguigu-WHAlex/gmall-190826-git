package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_EVENT_TOPIC))

    //时间转换
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将每一条元素转换为样例类对象
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map { case (_, value) =>

      val log: EventLog = JSON.parseObject(value, classOf[EventLog])

      //补充日期及小时字段
      val ts: Long = log.ts
      val dateStr: String = sdf.format(new Date(ts))
      log.logDate = dateStr.split(" ")(0)
      log.logHour = dateStr.split(" ")(1)

      log
    }

    //根据条件产生预警日志：同一设备，30秒内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。

    //5.开窗30秒 -> 30秒内
    val windowEventLogDStream: DStream[EventLog] = eventLogDStream.window(Seconds(30))

    //6.分组 -> 同一设备
    val midToLogIter: DStream[(String, Iterable[EventLog])] = windowEventLogDStream
      .map(log => (log.mid, log))
      .groupByKey()

    //7.判断是否使用三个用户ID && 是否有点击商品行为
    val boolToAlterInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIter.map { case (mid, logIter) =>

      //创建集合用于存放领取优惠券所对应的uid
      val uids = new util.HashSet[String]()
      //创建集合用于存放领取优惠券所对应的商品ID
      val itemIds = new util.HashSet[String]()
      //创建集合用于存放用户行为
      val events = new util.ArrayList[String]()

      //是否有浏览商品行为的标志位
      var noClick: Boolean = true

      //遍历log的迭代器
      breakable {
        logIter.foreach(log => {
          //添加用户行为
          events.add(log.evid)
          //判断是否存在浏览商品行为
          if ("clickItem".equals(log.evid)) {
            noClick = false
            break()
            //判断是否为领券行为
          } else if ("coupon".equals(log.evid)) {
            uids.add(log.uid)
            itemIds.add(log.itemid)
          }
        })
      }

      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }

    //8.过滤出真正的预警日志
    val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlterInfoDStream.filter(_._1).map(_._2)

    //9.转换数据结构->（id,CouponAlertInfo）
    val minToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfoDStream.map(log => {

      //获取时间戳
      val ts: Long = log.ts

      //获取分钟数
      val min: Long = ts / 1000 / 60

      (log.mid + min.toString, log)
    })

    //10.将数据写入ES
    minToAlertInfoDStream.foreachRDD(rdd => {

      //对分区写
      rdd.foreachPartition(midToLogIter => {
        MyEsUtil.insertBulk(GmallConstants.GMALL_ALERT_INFO_INDEX
          , midToLogIter.toList)
      })
    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
