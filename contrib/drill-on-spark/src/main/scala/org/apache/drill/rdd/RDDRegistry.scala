package org.apache.drill.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDRegistry[OUT<:DrillOutgoingRowType:ClassTag] extends Iterable[(String, RDD[OUT])] with Serializable {
  var name2rdd = Map[String, RDD[OUT]]()

  def register(name:String, rdd:RDD[OUT]): Unit = {
    name2rdd += (name.toUpperCase -> rdd)
  }

  def unregister(name:String): Unit = {
    name2rdd -= name.toUpperCase
  }

  def find(name:String): Option[RDD[OUT]] = {
    name2rdd.get(name)
  }

  def iterator = name2rdd.iterator
  def names = name2rdd.keys
  def rdds = name2rdd.values
}