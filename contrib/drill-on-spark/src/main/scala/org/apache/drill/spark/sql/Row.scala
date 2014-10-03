package org.apache.drill.spark.sql.sql

import java.io.{InputStream, Reader}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.drill.exec.expr.TypeHelper
import org.apache.drill.exec.record.VectorAccessible
import org.apache.drill.exec.vector.ValueVector
import org.apache.drill.exec.vector.accessor.SqlAccessor
import org.apache.drill.spark.SparkRowType

import scala.util.Try

trait Row extends SqlAccessor with SparkRowType

trait NumberedRow extends Row

trait NamedRow

class RowIterator(row:DrillRow) extends Iterator[Any] {
  val column = new AtomicInteger()

  override def hasNext: Boolean = column.get() < row.length

  override def next(): Any = {
    Try(row.accessor(column.getAndIncrement).getObject(row.row)) getOrElse None
  }
}


class DrillRow(@transient val va:VectorAccessible, val row:Int) extends Row with NumberedRow with NamedRow {

  protected[sql] def accessor(column:Int):SqlAccessor = {
    val vector = va.getValueAccessorById(null, column).getValueVector.asInstanceOf[ValueVector]
    accessor(vector)
  }

  protected def accessor(vector:ValueVector):SqlAccessor = {
    TypeHelper.getSqlAccessor(vector)
  }

  override def getTimestamp(column: Int): Timestamp = accessor(column).getTimestamp(row)

  override def getStream(column: Int): InputStream = accessor(column).getStream(row)

  override def getChar(column: Int): Char = accessor(column).getChar(row)

  override def getBigDecimal(column: Int): BigDecimal = accessor(column).getBigDecimal(row)

  override def getTime(column: Int): Time = accessor(column).getTime(row)

  override def getObject(column: Int): AnyRef = accessor(column).getObject(row)

  override def isNull(column: Int): Boolean = accessor(column).isNull(row)

  override def getDate(column: Int): Date = accessor(column).getDate(row)

  override def getReader(column: Int): Reader = accessor(column).getReader(row)

  override def getBytes(column: Int): Array[Byte] = accessor(column).getBytes(row)

  override def getDouble(column: Int): Double = accessor(column).getDouble(row)

  override def getFloat(column: Int): Float = accessor(column).getFloat(row)

  override def getLong(column: Int): Long = accessor(column).getLong(row)

  override def copy(): Row =  new DrillRow(va, row)

  override def getByte(column: Int): Byte = accessor(column).getByte(row)

  override def getBoolean(column: Int): Boolean = accessor(column).getBoolean(row)

  override def apply(column: Int): Any = getObject(column)

  override def getShort(column: Int): Short = accessor(column).getShort(row)

  override def getInt(column: Int): Int = accessor(column).getInt(row)

  override def isNullAt(column: Int): Boolean = isNull(row)

  override def getString(column: Int): String = accessor(column).getString(row)

  override def length: Int = va.getSchema.getFieldCount

  override def iterator: Iterator[Any] = {
    new RowIterator(this)
  }

  override def toString: String = {
    val sb = new StringBuilder
    val it = iterator
    while (it.hasNext) {
      sb.append(it.next).append(" -- ")
    }
    sb.toString
  }
}


