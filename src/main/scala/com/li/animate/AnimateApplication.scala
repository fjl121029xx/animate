package com.li.animate

import java.text.SimpleDateFormat
import java.util.{Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.util.control.Breaks._

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

object AnimateApplication {

  def main(args: Array[String]): Unit = {


    val aus: String = "{\"usa\":" +
      "[" +
      "{\"conditionKey\":\"subject\",\"isEqual\":1,\"conditionValue\":\"1,100100594\"}," +
      "{\"conditionKey\":\"area\",\"isEqual\":0,\"conditionValue\":\"-9\"}," +
      "{\"conditionKey\":\"terminal\",\"isEqual\":1,\"conditionValue\":\"2\"}]}"
    val logic: String = "0"
    val begDate: Long = 1530604662956L
    val endDate: Long = 1531795857526L
    val dateType: String = "shi"
    //
    //    if (args.length != 0) {
    //
    //      aus = args(0)
    //      logic = args(1)
    //      begDate = args(2)
    //      endDate = args(3)
    //      dateType = args(4)
    //    }

    val conf = new SparkConf().setMaster("local").setAppName("AnimateApplication")
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
    val animate = df.rdd.filter(filterFun).mapPartitions(mapTimeFun).toDS()
    animate.groupBy("loginTime").count().show()
    //    df.filter(filterFun).groupBy("area", "terminal", "subject").count().show()
    //    df.filter(filterFun).groupBy("area", "terminal", "subject")
    //      .count().repartition(1).rdd
    //      .saveAsTextFile("hdfs://192.168.100.26:8020/ES_Test_DATA/" + (new util.Random).nextInt(5) + "")


  }


}
