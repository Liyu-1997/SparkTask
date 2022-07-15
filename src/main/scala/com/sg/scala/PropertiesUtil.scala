package com.sg.scala

import java.util.Properties
import java.util.function.BiConsumer
import scala.collection.mutable

object PropertiesUtil {

  def propToMap(prop: Properties): mutable.Map[String, Object] = {
    val map = mutable.Map[String, Object]()
    prop.forEach(new BiConsumer[Any, Any] {
      override def accept(t: Any, u: Any): Unit = map.put(t.toString, u.toString)
    })
    map
  }

}
