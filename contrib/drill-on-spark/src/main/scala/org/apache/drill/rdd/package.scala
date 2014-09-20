package org.apache.drill

package object rdd {
  type SparkRowType = org.apache.spark.sql.Row
  type DrillRowType = org.apache.drill.rdd.sql.Record
}
