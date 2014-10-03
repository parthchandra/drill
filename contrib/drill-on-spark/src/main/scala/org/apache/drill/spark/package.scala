package org.apache.drill

import org.apache.drill.spark.sql.sql.RecordInfo

import scala.reflect.ClassTag

package object spark {
  type SparkRowType = org.apache.spark.sql.Row
  type DrillIncomingRowType = org.apache.drill.spark.sql.sql.Record
  type DrillOutgoingRowType = org.apache.drill.spark.sql.sql.ComplexRecord
  type QueryManagerFactoryType[T] = () => org.apache.drill.spark.sql.query.QueryManager[T]
  type RecordFactoryType[T] = (RecordInfo) => T
//  type DrillContextType = org.apache.drill.rdd.DrillContext[DrillIncomingRowType, DrillOutgoingRowType]
}
