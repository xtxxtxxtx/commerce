package area

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class Employee(name : String, salary : Long)
case class Average(var sum : Long, var count : Long)

class MyAverage extends Aggregator[Employee, Average, Double]{

  //计算并返回最终的聚合结果
  override def zero: Average = Average(0L, 0L)

  //根据传入的参数值更新buffer值
  override def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  //合并两个buffer值，将buffer2合并到buffer1
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  //计算输出
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  /**
    * 设定中间值类型的编码器需要转换成case类
    * Encoders.product是将scala元组和case类转换的编码器
    */
  override def bufferEncoder: Encoder[Average] = Encoders.product

  //设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}