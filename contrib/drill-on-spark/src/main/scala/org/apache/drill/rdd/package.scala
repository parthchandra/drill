package org.apache.drill

import org.apache.drill.rdd.complex.ReadableRecordInfo

package object rdd {
  type SparkRowType = org.apache.spark.sql.Row
  type DrillIncomingRowType = org.apache.drill.rdd.complex.ReadableRecord
  type DrillOutgoingRowType = org.apache.drill.rdd.complex.CObject
  type QueryManagerFactoryType[T] = () => org.apache.drill.rdd.complex.query.QueryManager[T]
  type RecordFactoryType[T] = (ReadableRecordInfo) => T
//  type DrillContextType = org.apache.drill.rdd.DrillContext[DrillIncomingRowType, DrillOutgoingRowType]

}
