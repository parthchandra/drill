package org.apache.drill.rdd.complex

import java.util

import io.netty.buffer.DrillBuf
import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.expr.TypeHelper
import org.apache.drill.exec.expr.holders.{Float8Holder, BitHolder, Decimal18Holder, VarCharHolder}
import org.apache.drill.exec.memory.{BufferAllocator, TopLevelAllocator}
import org.apache.drill.exec.physical.impl.OutputMutator
import org.apache.drill.exec.record.MaterializedField
import org.apache.drill.exec.vector.ValueVector
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter
import org.apache.drill.exec.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.drill.exec.vector.complex.writer.{BaseWriter, FieldWriter}
import org.apache.drill.rdd.DrillOutgoingRowType

import scala.collection.mutable

class ComplexRecordWriter[T<:DrillOutgoingRowType] {
  val config = DrillConfig.createClient()
  val allocator = new TopLevelAllocator(config)
  val mutator = new CachingMutator(allocator)
  val writer = new VectorContainerWriter(mutator)
  val varCharFactory = new HolderFactory(allocator)
  private var count = 0

  def vector = writer.getMapVector

  protected def serialize(writer:BaseWriter, value:CValue): Unit = {
    value match {
      case CNothing =>
      // todo: how do you write nothing?
      case CNull =>
      // todo: how do you write null?
      case CString(value) => {
        val holder  = varCharFactory.createVarCharHolder(value)
        writer.asInstanceOf[FieldWriter].write(holder)
        holder.buffer.release()
      }
      case CNumber(value:BigDecimal) => {
        val holder = new Float8Holder
        holder.value = value.doubleValue
        writer.asInstanceOf[FieldWriter].write(holder)
      }
      case CBool(value) => {
        val holder = new BitHolder
        holder.value = value match {
          case true => 1
          case _ => 0
        }
        writer.asInstanceOf[FieldWriter].write(holder)
      }
      case CArray(values) => {
        val listWriter = writer.asInstanceOf[ListWriter]
        listWriter.start()
        values.foreach {
          case value:CObject =>
            serialize(listWriter.map(), value)
          case value:CArray =>
            serialize(listWriter.list(), value)
          case value:CString =>
            serialize(listWriter.varChar(), value)
          case value:CNumber =>
            serialize(listWriter.float8(), value)
          case value:CBool =>
            serialize(listWriter.bit(), value)
          case _ =>
          //TODO: how do we represent nothing, null?
        }
        listWriter.end()
      }
      case CObject(fields) => {
        val mapWriter = writer.asInstanceOf[MapWriter]
        mapWriter.start()
        fields.foreach {
          case (name, value:CObject) =>
            serialize(mapWriter.map(name), value)
          case (name, value:CArray) =>
            serialize(mapWriter.list(name), value)
          case (name, value:CString) =>
            serialize(mapWriter.varChar(name), value)
          case (name, value:CNumber) =>
            serialize(mapWriter.float8(name), value)
          case (name, value:CBool) =>
            serialize(mapWriter.bit(name), value)
          case _ =>
          //TODO: how do we represent nothing, null?
        }
        mapWriter.end()
      }
    }
  }

  def write(record:T): Unit = {
    serialize(writer.rootAsMap, record)
    count += 1
    writer.setValueCount(count)
    writer.setPosition(count)
  }

  def close(): Unit = {
    writer.clear()
    allocator.close()
  }
}

class HolderFactory(allocator:BufferAllocator) {

  def createVarCharHolder(text:String) = {
    val holder = new VarCharHolder
    holder.buffer = allocator.buffer(text.getBytes.length)
    holder.buffer.setBytes(0, text.getBytes)
    holder.start = 0
    holder.end = text.getBytes.length
    holder
  }

}

class CachingMutator(allocator:BufferAllocator) extends OutputMutator {
  val vectors = mutable.HashMap[MaterializedField, ValueVector]()

  override def addField[T <: ValueVector](field: MaterializedField, clazz: Class[T]): T = {
    vectors.getOrElseUpdate(field, TypeHelper.getNewVector(field, allocator)).asInstanceOf[T]
  }

  override def addFields(vvList: util.List[ValueVector]): Unit = ???

  override def allocate(recordCount: Int): Unit = ???

  override def isNewSchema: Boolean = ???

  override def getManagedBuffer: DrillBuf = ???
}

