package org.apache.drill.rdd

import java.util.concurrent.TimeUnit

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.complex.reader.BaseReader
import org.apache.drill.exec.vector.complex.reader.BaseReader.{ListReader, MapReader}
import org.apache.drill.rdd.complex.query.{QueryContext, StreamingQueryManager}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag

import org.apache.drill.rdd.complex._

object DrillConversions {

  /*
   This method is called once per partition during compute phase.
  */
  type IN = DrillIncomingRowType
  type OUT = DrillOutgoingRowType

  val registry = new RDDRegistry[OUT]
  // introduce client connection here.
  // exclude implicit here.
  //

  implicit class RichSparkContext(sc: SparkContext) {

    def getRegistry() = registry

    def drillRDD(sql: String): DrillRDD[IN, OUT] = {
      new DrillRDD[IN, OUT](sc, DrillContext(sql, registry, true))
    }

    def registerAsTable(name:String, rdd:RDD[OUT]): Unit = {
      registry.register(name, rdd)
    }
  }

  implicit def toCString(value:String) = CString(value)
  implicit def toCNumber(value:Int) = CNumber(value)
  implicit def toCNumber(value:Long) = CNumber(value)
  implicit def toCNumber(value:Double) = CNumber(value)
  implicit def toCBool(value:Boolean) = CBool(value)
  implicit def toCArray(value:Seq[CValue]) = CArray(value: _*)
}
