package session2

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  //保存所有聚合数据
  private val statMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = statMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val statAccumulator = new SessionStatAccumulator
    statMap.synchronized{
      statAccumulator.statMap ++= this.statMap
    }
    statAccumulator
  }

  override def reset(): Unit = statMap.clear()

  override def add(v: String): Unit = {
    if (!statMap.contains(v)){
      statMap += (v -> 0)
    }
    statMap.update(v, statMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc : SessionStatAccumulator => {
        (this.statMap /: acc.value) {
          case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = this.statMap
}