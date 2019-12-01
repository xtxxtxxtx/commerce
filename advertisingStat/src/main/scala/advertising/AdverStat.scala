package advertising

import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafkaBrokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafkaTopic = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_ljy",

      /**
        * auto.offset.reset
        *   latest：先去Zookeeper中获取offset，如果有直接使用，如果没有从最新的数据开始消费
        *   earlist：先去Zookeeper中获取offset，如果有直接使用，如果没有从最新的数据开始消费
        *   none：先去Zookeeper中获取offset，如果有直接使用，如果没有直接报错
        */
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false : java.lang.Boolean)
    )

    //adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParam))

    /**
      * 取出了DStream里面每一条数据的value值
      * adReadTimeValueDStream : Dstram[RDD  RDD  RDD ...]   RDD[String]
      * String:  timestamp province city userid adid
      */
    val adReadTimeValueDStream = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream = adReadTimeValueDStream.transform {

      logRDD =>
        //blackListArray: Array[AdBlacklist]     AdBlacklist: userId
        val blackListArray = AdBlacklistDAO.findAll()

        //userIdArray: Array[Long]  [userId1, userId2, ...]
        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter {
          // log : timestamp province city userid adid
          case log =>
            val logSplit = log.split(" ")
            val userid = logSplit(3).toLong
            !userIdArray.contains(userid)
        }
    }

    streamingContext.checkpoint("./sparkStreaming")
    adRealTimeFilterDStream.checkpoint(Duration(10000))

//    adRealTimeFilterDStream.map(rdd => rdd.foreach(println(_)))

    //需求一：实现维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    //需求二:各省各城市一天中广告点击量(累计统计)
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    //需求三:一天中各省广告点击量Top3的
    provinceTop3Advertising(sparkSession, key2ProvinceCityCountDStream)

    //需求四:最近一个小时广告点击量
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
    * 最近一个小时广告点击量
    */
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2HourMinuteDStream = adRealTimeFilterDStream.map{
      //log : timestamp province city userid adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        //yyyyMMddHHmm
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4).toLong

        val key = timeMinute + "_" + adid
        (key, 1L)
    }

    val key2WindowDStream = key2HourMinuteDStream.reduceByKeyAndWindow((l : Long, j : Long) => l + j, Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items =>
            val treadArray = new ArrayBuffer[AdClickTrend]()
            for ((key, count) <- items){
              val keySplit = key.split("_")
              val date = keySplit(0).substring(0,8)
              val hour = keySplit(0).substring(8, 10)
              val minute = keySplit(0).substring(10)
              val adid = keySplit(1).toLong

              treadArray += AdClickTrend(date, hour, minute, adid, count)
            }

            AdClickTrendDAO.updateBatch(treadArray.toArray)
        }
    }
  }

  /**
    * 一天中各省广告点击量Top3的
    */
  def provinceTop3Advertising(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {

      //key2ProvinceCityCountDStream: [RDD[(key, count)]]
      //key: date_province_city_adid
      //key2ProvinceCountDStream: [RDD[(newKey, count)]]
      //newKey: date_province_adid
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map{
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey ,count)
    }

    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_+_)

    val top3DStream = key2ProvinceAggrCountDStream.transform{
      rdd =>
        // rdd:RDD[(key, count)]
        // key: date_province_adid
        val basucDateRDD = rdd.map{
          case (key, count) =>
            val date = key.split("_")(0)
            val province = key.split("_")(1)
            val adid = key.split("_")(2).toLong

            (date, province, adid, count)
        }

        import sparkSession.implicits._
        basucDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adid, count from " +
          "(select date, province, adid, count, row_number() " +
          "over(partition by date, province order by count desc) rank from tmp_basic_info) t where rank <= 3"

        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }

  /**
    * 各省各城市一天中广告点击量(累计统计)
    */
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String])= {

    //adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        //yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4).toLong

        val key = dateKey + "_" + province + "_" + city + "_" + adid

        (key, 1L)
    }

    //key2StateDStream:一天一个省一个城市中某个广告的点击量
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long]{
      (values : Seq[Long], state : Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get

        for (value <- values){
          newValue += value
        }

        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()

          // key: date_province_city_adid
          for ((key, count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong

            adStatArray += AdStat(date, province, city, adid, count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }
    key2StateDStream
  }

  /**
    * 实现维护黑名单
    */
  def generateBlackList(adRealTimeFilterDStream: DStream[String]) {

    //adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    //key2NumDStream: [RDD[(key, 1L)]]
    val key2NumDStream = adRealTimeFilterDStream.map {
      //  log : timestamp province city userid adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong

        val key = dateKey + "_" + userId + "_" + adid

        (key, 1L)
    }

    val key2CountDStream = key2NumDStream.reduceByKey(_+_)

    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for ((key, count) <- items){
            val date  = key.split("_")(0)
            val userId = key.split("_")(1).toLong
            val adid = key.split("_")(2).toLong

            clickCountArray += AdUserClickCount(date, userId, adid, count)
          }

          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    //ket2BlackListDStream : DStream[RDD(key, count)]
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val date = key.split("_")(0)
        val userId = key.split("_")(1).toLong
        val adid = key.split("_")(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        if (clickCount > 100)
          true
        else
          false
    }

    val userIdStream = key2BlackListDStream.map{
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for (userId <- items){
            userIdArray += AdBlacklist(userId)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }
  }
}