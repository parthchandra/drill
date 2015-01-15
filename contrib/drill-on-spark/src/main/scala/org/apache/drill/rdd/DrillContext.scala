package org.apache.drill.rdd

import org.apache.drill.rdd.DrillType.{OUTTYPE, INTYPE}
import org.apache.drill.rdd.complex.query.ExtendedDrillClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DrillContext {
  var sc: SparkContext = null
  var registry: RDDRegistry[OUTTYPE] = null
  var drillClient: ExtendedDrillClient = null

  def this(sc_ :SparkContext, drillClient_ : ExtendedDrillClient = null, registry_ : RDDRegistry[OUTTYPE] = null) {
    this()
    sc = sc_
    drillClient = drillClient_ match {
      case null => new ExtendedDrillClient()
      case _ =>  drillClient_
    }
    registry = registry_ match {
      case null => new RDDRegistry[OUTTYPE]
      case _ => registry_
    }
  }

  def drill(sql: String): DrillRDD[INTYPE, OUTTYPE] = {
    new DrillRDD[INTYPE, OUTTYPE](this, sql, true)
  }

  def registerAsTable(name:String, rdd:RDD[OUTTYPE]): Unit = {
    registry.register(name, rdd)
  }

  def getClient = drillClient

  def stop = drillClient.close

  def getSparkContext = sc

  def getRegistry() = registry
}
