package com.li.animate.redisutil


import com.li.animate.UaList
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import redis.clients.jedis._

import scala.util.control.Breaks._

class RedisClient {

}

object RedisClient {

  def main(args: Array[String]): Unit = {

    val aus: String = "{\"usa\":" +
      "[" +
      "{\"conditionKey\":\"subject\",\"isEqual\":1,\"conditionValue\":\"1,100100594\"}," +
      "{\"conditionKey\":\"area\",\"isEqual\":0,\"conditionValue\":\"-9\"}," +
      "{\"conditionKey\":\"terminal\",\"isEqual\":1,\"conditionValue\":\"2\"}]}"
    val logic: String = "0"
    val begDate: Long = 1530604662956L
    val endDate: Long = 1531795857526L
    val dateType: String = "ri"


    val jr: Jedis = null;
    try {
      //      val jr = new Jedis("192.168.100.26", 6379);
      //            jr.auth("test123");
      //      System.out.println(jr.ping());
      //      System.out.println(jr.isConnected() && jr.ping().equals("PONG"));
      //      val key = "bjsxtgaga";
      //
      //      jr.set(key, "hello redis!");

      //    breakable{
      //
      //      while (true) {
      //
      //        val v = jr.get(aus + "+" + logic + "+" + begDate + "+" + endDate + "+" + dateType);
      //        System.out.println(v);
      //      }
      //    }
      //      jr.del(aus + "+" + logic + "+" + begDate + "+" + endDate + "+" + dateType)
      println(aus)
      implicit val formats = Serialization.formats(ShortTypeHints(List()))
      val obj: UaList = parse(aus).extract[UaList]
      println(obj)

//      {"usa":[{"conditionKey":"subject","isEqual":1,"conditionValue":"1,100100594"},{"conditionKey":"area","isEqual":0,"conditionValue":"-9"},{"conditionKey":"terminal","isEqual":1,"conditionValue":"2"}]}
//      UaList(List(UserAnimate(subject,1,1,100100594), UserAnimate(area,0,-9), UserAnimate(terminal,1,2)))
    } catch {
      case t => // todo: handle error
        t.printStackTrace()
    } finally {
      if (jr != null) {
        jr.disconnect()
      }
    }

  }

}
