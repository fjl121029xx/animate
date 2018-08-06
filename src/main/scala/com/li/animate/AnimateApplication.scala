package com.li.animate

import java.text.SimpleDateFormat
import java.util.Date


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.util.control.Breaks._

import redis.clients.jedis._

case class UserAnimate(conditionKey: String,
                       isEqual: Int,
                       conditionValue: String
                      ) extends Serializable

case class UaList(usa: Seq[UserAnimate]) extends Serializable

case class Animate(area: Long,
                   enventName: String,
                   loginTime: String,
                   subject: Long,
                   terminal: String,
                   userName: String) extends Serializable

class AnimateApplication {

}


object AnimateApplication {
  def parseDouble(s: String): Option[Long] = try {
    Some(s.toLong)
  } catch {
    case _ => None
  }

  def main(args: Array[String]): Unit = {

    implicit var taskId: String = ""

    implicit var aus: String = ""

    implicit var logic: String = ""
    implicit var begDate: Long = 0L
    implicit var endDate: Long = 0L
    implicit var dateType: String = ""
    //
    if (args.length != 0) {

      taskId = args(0)
    }
    println(taskId);
    val jr = new Jedis("192.168.100.26", 6379);
    aus = jr.get(taskId + "=aus")
    logic = jr.get(taskId + "=logic")
    begDate = jr.get(taskId + "=begDate").toLong
    endDate = jr.get(taskId + "=endDate").toLong
    dateType = jr.get(taskId + "=dateType")

    val conf = new SparkConf()
//      .setMaster("local")
      .setAppName("AnimateApplication")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Animate], classOf[UaList]))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://192.168.100.26:8020/ES_Test_DATA/*.json")

    val filterFun = (row: Row) => {

      var flag: Boolean = true

      implicit val formats = Serialization.formats(ShortTypeHints(List()))
      val obj: UaList = parse(aus).extract[UaList]

      if (logic.equals("1")) {

        breakable {
          for (x <- obj.usa) {

            val conVal = x.conditionValue
            val isEqu = x.isEqual
            //
            val value = row.getAs(x.conditionKey).toString
            if (isEqu == 0) { //不包含
              flag = !conVal.contains(value)
              if (!flag) {
                break
              }
            } else if (isEqu == 1) {
              flag = conVal.contains(value)

              if (!flag) {
                break
              }
            }
          }
        }
      } else if (logic.equals("0")) {
        //或
        breakable {
          for (x <- obj.usa) {

            val conVal = x.conditionValue
            val isEqu = x.isEqual

            val value = row.getAs(x.conditionKey).toString
            if (isEqu == 0) { //不包含

              flag = !conVal.contains(value)


            } else if (isEqu == 1) {
              //包含
              flag = conVal.contains(value)

            }
          }
        }
      }

      val loginTime = row.getLong(2)

      flag = (loginTime >= begDate) && (loginTime <= endDate)
      flag
    }


    val mapTimeFun = (itr: Iterator[Row]) => {

      var format = new SimpleDateFormat("yyyy-MM-dd")

      if (dateType.equals("yue")) {
        format = new SimpleDateFormat("yyyy-MM")
      } else if (dateType.equals("shi")) {
        format = new SimpleDateFormat("yyyy-MM-dd HH")
      } else if (dateType.equals("zhou")) {
        format = new SimpleDateFormat("ww周")
      }

      var ite: List[Animate] = List()

      while (itr.hasNext) {
        val row = itr.next()
        val loginTime = row.getLong(2)

        val date = new Date(loginTime)


        ite = Animate(row.getLong(0), row.getString(1), format.format(date), row.getLong(3), row.getString(4), row.getString(5)) +: ite

      }

      ite.iterator
    }

    //    flag
    import sqlContext.implicits._

    val str = new StrAppend
    sc.register(str)

    val animate = df.coalesce(1).rdd.filter(filterFun)
      .mapPartitions(mapTimeFun)
      .map(an => (an.loginTime, 1))
      .reduceByKey(_ + _).coalesce(1,true)
      .foreachPartition((ite: Iterator[(String, Int)]) => {

        while (ite.hasNext) {
          val i = ite.next()

          val tmp = i._1.concat("=").concat(i._2.toString).concat("|")
          str.add(tmp)
        }
      })

    val key = aus + "+" + logic + "+" + begDate + "+" + endDate + "+" + dateType
    jr.set(key, str.value.toString())
    jr.expire(key, 30 * 60)

    sc.stop()
  }


}
