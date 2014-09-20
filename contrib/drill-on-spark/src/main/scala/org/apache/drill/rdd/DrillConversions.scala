package org.apache.drill.rdd

import java.util.concurrent.TimeUnit

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.complex.MapVector
import org.apache.drill.exec.vector.complex.reader.BaseReader
import org.apache.drill.exec.vector.complex.reader.BaseReader.{ListReader, MapReader}
import org.apache.drill.rdd.query.StreamingQueryManager
import org.apache.drill.rdd.sql.DrillRecord
import org.apache.spark.SparkContext

import scala.concurrent.duration.{Duration, FiniteDuration}

object DrillConversions {

  private val recordFactory = (vector:MapVector, row:Int) => {
    new DrillRecord(vector.getAccessor.getReader, row)
  }

  /*
   This method is called once per partition during compute cycle.
  */
  private val engineFactory = () => {
      val conf = DrillConfig.createClient()
      val allocator = new TopLevelAllocator(conf)
      val loader = new RecordBatchLoader(allocator)
      new StreamingQueryManager[DrillRowType](loader, recordFactory)
  }

  implicit class DrillSparkContext(sc: SparkContext) {
    def drillRDD(sql: String): DrillRDD[DrillRowType] = {
      new DrillRDD[DrillRowType](sc, sql, engineFactory)
    }
  }

  implicit class DrillReader(reader:BaseReader) {
    def :>(name:String): MapReader = {
      reader.asInstanceOf[MapReader].reader(name).asInstanceOf[MapReader]
    }

    def ::>(name:String): ListReader = {
      reader.asInstanceOf[MapReader].reader(name).asInstanceOf[ListReader]
    }

    def /(name: String): BaseReader = {
      reader.asInstanceOf[MapReader].reader(name)
    }
  }

  implicit class RichTime(value:Long) {
    def seconds(): Duration = new FiniteDuration(value, TimeUnit.SECONDS)
    def millis(): Duration = new FiniteDuration(value, TimeUnit.MILLISECONDS)
  }

}
