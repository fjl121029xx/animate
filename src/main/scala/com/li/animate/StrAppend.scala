package com.li.animate

import org.apache.spark.util.AccumulatorV2

class StrAppend extends AccumulatorV2[String, String] {


  var str = ""

  override def isZero: Boolean = {

    str == ""
  }

  override def copy(): AccumulatorV2[String, String] = {

    val strAppend = new StrAppend
    strAppend.str = this.str

    strAppend
  }

  override def reset(): Unit = {

    str = ""
  }

  override def add(v: String): Unit = {

    str = str + v.toString
  }

  override def merge(o: AccumulatorV2[String, String]): Unit = o match {

    case o: StrAppend => str += o.str

    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${o.getClass.getName}")

  }

  override def value: String = str
}
