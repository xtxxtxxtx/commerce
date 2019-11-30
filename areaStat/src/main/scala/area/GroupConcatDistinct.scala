package area

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class GroupConcatDistinct extends UserDefinedAggregateFunction{

  //UDAF：输入类型是String
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  //缓冲区类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  //输出类型是String
  override def dataType: DataType = StringType

  // 一致性检验，如果为true那么输入不变的情况下结果也是不变的
  override def deterministic: Boolean = true

  /**
    * 设置聚合中间buffer的中间值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  //用输入数据input更新buffer值，类似于combineByKey
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityInfo = input.getString(0)
    var bufferCityInfo = buffer.getString(0)

    if (!bufferCityInfo.contains(cityInfo)){
      if ("".equals(bufferCityInfo)){
        bufferCityInfo += cityInfo
      }else{
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  /**
    * 合并这两个buffer，将buffer2合并到buffer1，在合并两个分区聚合结果时候会用到，类似于reduceByKey
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    // bufferCityInfo2: cityId1:cityName1, cityId2:cityName2
    val bufferCityInfo2 = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")){
      if (!bufferCityInfo1.contains(cityInfo)){
        if ("".equals(bufferCityInfo1)){
          bufferCityInfo1 += cityInfo
        }else{
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }
    buffer1.update(0, bufferCityInfo1)
  }

  //计算并返回最终的聚合结果
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}