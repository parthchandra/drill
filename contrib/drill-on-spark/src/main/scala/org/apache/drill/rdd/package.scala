package org.apache.drill

import org.apache.drill.rdd.complex.FieldReaderInfo

package object rdd {
  type DrillIncomingRowType = org.apache.drill.rdd.complex.ReadableRecord
  type DrillOutgoingRowType = org.apache.drill.rdd.complex.CObject
  type QueryManagerFactoryType[T] = () => org.apache.drill.rdd.complex.query.QueryManager[T]
  type RecordFactoryType[T] = (FieldReaderInfo) => T
}
