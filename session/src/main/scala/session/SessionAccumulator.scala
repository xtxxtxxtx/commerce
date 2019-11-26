package session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累计器
  */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  private val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val accumulator = new SessionAccumulator
    accumulator.countMap ++= this.countMap
    accumulator
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!countMap.contains(v)){
      this.countMap += (v -> 0)
    }
    this.countMap.update(v, countMap(v) + 1)
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      // (0 /: (1 to 100))(_+_)
      // (0 /: (1 to 100)){case (int1, !) => int1 + int2}
      // (1 /: 100).foldLeft(0)
      // (this.countMap /: acc.countMap)
      case acc : SessionAccumulator  => acc.countMap.foldLeft(countMap){
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}