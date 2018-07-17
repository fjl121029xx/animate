package com.li.animate.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

@JsonIgnoreProperties(ignoreUnknown = true)
case class UserAnimate(val conditionKey: String,
                       val isEqual: Int,
                       val conditionValue: String
                      )

@JsonIgnoreProperties(ignoreUnknown = true)
case class UaList(val usa: Seq[UserAnimate])



object UserAnimate {

  def main(args: Array[String]): Unit = {


    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)


    val json = "{\"usa\":[{\"conditionKey\":\"subject\",\"isEqual\":1,\"conditionValue\":\"2\"}," +
      "{\"conditionKey\":\"area\",\"isEqual\":0,\"conditionValue\":\"-9\"}," +
      "{\"conditionKey\":\"terminal\",\"isEqual\":1,\"conditionValue\":\"2\"}]}"


    var obj = mapper.readValue(json, classOf[UaList])



    for ( x <- obj.usa ) {

      println(x)
    }

  }
}
