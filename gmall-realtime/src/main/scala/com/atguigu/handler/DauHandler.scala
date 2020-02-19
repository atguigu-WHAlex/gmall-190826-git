package com.atguigu.handler

import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  /**
    * 同批次去重
    *
    * @param filterByRedis 根据Redis中的数据局过滤后的结果
    */
  def filterDataByBatch(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {

    //a.将数据转换为((date,mid),log)
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedis.map(log =>
      ((log.logDate, log.mid), log)
    )

    //b.按照key分组
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //c.组内按照日期排序，取第一个元素
    //    val value1: DStream[List[StartUpLog]] = dateMidToLogIterDStream.map { case ((_, _), logIter) =>
    //      val logs: List[StartUpLog] = logIter.toList.sortWith(_.logDate < _.logDate).take(1)
    //      logs
    //    }
    val value: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap { case ((_, _), logIter) =>
      val logs: List[StartUpLog] = logIter.toList.sortWith(_.ts < _.ts).take(1)
      logs
    }

    //d.返回数据
    value
  }


  /**
    * 跨批次去重(根据Redis中的数据进行去重)
    *
    * @param startLogDStream 从Kafka读取的原始数据
    */
  def filterDataByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    startLogDStream.transform(rdd => {

      //查询Redis数据，使用广播变量发送给Executor
      //获取连接
      //      val jedisClient: Jedis = RedisUtil.getJedisClient
      //      val ts: Long = System.currentTimeMillis()
      //date->今天/昨天
      //      val strings: util.Set[String] = jedisClient.smembers("dau:yesterdaydate")
      //      val strings1: util.Set[String] = jedisClient.smembers("dau:todaydate")
      //      Map[String, util.Set[String]]("yes" -> strings, "today" -> strings1)
      //释放连接
      //      jedisClient.close()

      rdd.mapPartitions(iter => {
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //过滤
        val logs: Iterator[StartUpLog] = iter.filter(log => {
          val redisKey = s"dau:${log.logDate}"
          !jedisClient.sismember(redisKey, log.mid)
        })
        //释放连接
        jedisClient.close()
        logs
      })
    })

  }


  /**
    * 将2次过滤后的数据集中的mid保存至Redis
    *
    * @param startLogDStream 经过2次过滤后的数据集
    */
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {

    startLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //将数据写入Redis
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //释放连接
        jedisClient.close()

      })

    })

  }

}
