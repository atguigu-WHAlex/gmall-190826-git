package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.分别消费三个主题的数据
    val orderInfoKafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))
    val orderDetailKafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
    val userInfoKafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_USER_INFO_TOPIC))

    //4.将orderInfoKafkaDStream和orderDetailKafkaDStream中每一个元素转换为样例类对象
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map { case (_, value) =>
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
      //处理创建日期及小时 2020-02-21 12:12:12
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      //手机号脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"
      //返回数据
      orderInfo
    }.map(orderInfo => (orderInfo.id, orderInfo))

    val orderIdToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map { case (_, value) =>
      JSON.parseObject(value, classOf[OrderDetail])
    }.map(orderDetail => (orderDetail.order_id, orderDetail))

    //5.将订单数据和订单详情数据JOIN
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(orderIdToOrderDetailDStream)

    //6.处理JOIN之后的数据
    val orderInfoAndDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions(iter => {

      //定义集合用于存放JOIN上的数据
      val list = new ListBuffer[SaleDetail]()
      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

      //处理每一条数据
      iter.foreach { case (orderId, (orderInfoOpt, orderDetailOpt)) =>

        //定义info及detail数据的RedisKey
        val orderRedisKey = s"order:$orderId"
        val detailRedisKey = s"detail:$orderId"

        //一、判断orderInfoOpt是否为空
        if (orderInfoOpt.isDefined) {
          //取出orderInfoOpt数据
          val orderInfo: OrderInfo = orderInfoOpt.get

          //1.orderInfoOpt有数据,则判断orderDetailOpt是否有数据
          if (orderDetailOpt.isDefined) {
            //orderDetailOpt有数据,取出数据
            val orderDetail: OrderDetail = orderDetailOpt.get
            //集合数据并添加至集合
            list += new SaleDetail(orderInfo, orderDetail)
          }

          //2.将orderInfo转换为JSON字符串写入Redis  String  s"order:$order_id",json
          //val str: String = JSON.toJSONString(orderInfo)
          val orderJson: String = Serialization.write(orderInfo)
          jedisClient.set(orderRedisKey, orderJson)
          jedisClient.expire(orderRedisKey, 300)

          //3.查询detail缓存,如果存在数据则JOIN放入集合 set
          val orderDetailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          import scala.collection.JavaConversions._
          orderDetailSet.foreach(detailJson => {
            //将detailJson转换为OrderDetail对象
            val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
            list += new SaleDetail(orderInfo, detail)
          })

          //二、orderInfoOpt没有数据
        } else {

          //直接获取orderDetail数据
          val orderDetail: OrderDetail = orderDetailOpt.get

          //查询Redis取出OrderInfo的数据
          if (jedisClient.exists(orderRedisKey)) {
            //Redis中存在OrderInfo,读取数据JOIN之后放入集合
            val orderJson: String = jedisClient.get(orderRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])

            list += new SaleDetail(orderInfo, orderDetail)

          } else {
            //Redis中不存在OrderInfo,将Detail数据写入Redis
            val detailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, detailJson)
            jedisClient.expire(detailRedisKey, 300)
          }
        }
      }

      //关闭Redis连接
      jedisClient.close()
      list.toIterator
    })

    //测试打印
    orderInfoAndDetailDStream.print(100)

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
