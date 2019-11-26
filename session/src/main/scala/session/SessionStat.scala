package session

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object SessionStat {


  def main(args: Array[String]): Unit = {

    //获取jsonStr
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    //将jsonStr转换为jsonObject
    val taskParam = JSONObject.fromObject(jsonStr)

    //创建全局唯一主键
    val taskUUID = UUID.randomUUID().toString

    //创建sparkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("session")

    //创建sparkSession(包含sparkContext)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //拿到原始数据 actionRDD:RDD[UserVisitAction]
    val actionRDD = getOriAction(sparkSession, taskParam)

    //sessionId2AtcionRDD : RDD[(sessionId, UserVisitAction)]
    val sessionId2AtcionRDD = actionRDD.map(item =>(item.session_id, item))

    //session2GroupActionRDD: RDD[(sessionId, iterable_UserVisitAction)]
    val sessionId2GroupActionRDD = sessionId2AtcionRDD.groupByKey()

    sessionId2GroupActionRDD.cache()

    //userId2AggrInfoRDD: RDD[(userId, aggrInfo)]
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupActionRDD)

    //将自定义的累计器注册进来
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    //sessionId2FilterRDD:RDD[(sessionId, fullInfo)]  所有符合过滤条件的数据组成的RDD
    //实现根据条件对session数据进行过滤，并完成3累加器的更新
    val sessionId2FilterRDD = getSessionFilterInfo(taskParam, sessionId2FullInfoRDD, sessionAccumulator)

    sessionId2FilterRDD.foreach(println(_))

    //占比计算与统计写入mysql
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRationRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    sessionRationRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat")
      .mode(SaveMode.Append)
      .save()
  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3){
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if (visitLength >= 4 && visitLength <= 6){
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9){
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    }else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (stepLength >=1 && stepLength <= 3){
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  //得到过滤后的数据
  def getSessionFilterInfo(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    //拼接过滤条件
    var fiterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS +"|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS else "")

    if (fiterInfo.endsWith("\\|")) {
      fiterInfo = fiterInfo.substring(0, fiterInfo.length - 1)
    }

    sessionId2FullInfoRDD.filter{
      case (sessionId, fullInfo) => {
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, fiterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
          success = false
        }else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, fiterInfo, Constants.PARAM_PROFESSIONALS)){
          success = false
        }else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, fiterInfo, Constants.PARAM_CITIES)){
          success = false
        }else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, fiterInfo, Constants.PARAM_SEX)){
          success = false
        }else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, fiterInfo, Constants.PARAM_KEYWORDS)){
          success = false
        }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, fiterInfo, Constants.PARAM_CATEGORY_IDS)){
          success = false
        }

        //使用累计器进行累加
        if (success){
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
      }
    }
  }

  //得到userId2AggrInfoRDD和userId2InfoRDD两张表联合的数据
  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) ={

    //userId2AggrInfoRDD: RDD[(userId, aggrInfo)]
    val userId2AggrInfoRDD = sessionId2GroupActionRDD.map{

      case (sessionId, iterableAction) =>

        var userId = -1L

        var startTime : Date = null
        var endTime : Date = null

        //步长
        var stepLength = 0

        val searchKeywords = new StringBuilder("")
        val clickCategoryIds = new StringBuilder("")

        for (action <- iterableAction){
          if (userId == -1L){
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)){
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)){
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString().contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId == -1 && !clickCategoryIds.toString().contains(clickCategoryId)) {
            clickCategoryIds.append(clickCategoryId + ",")
          }
          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString())
        val clickCds = StringUtils.trimComma(clickCategoryIds.toString())

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCds + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info"

    import sparkSession.implicits._
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    val sessionId2FullInfo = userId2AggrInfoRDD.join(userId2InfoRDD).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val FullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, FullInfo)
    }
    sessionId2FullInfo
  }

  //得到日期在startDate和endDate之间的数据
  def getOriAction(sparkSession: SparkSession, taskParam: JSONObject) = {

    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date > '" + startDate + "'and date < '" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}