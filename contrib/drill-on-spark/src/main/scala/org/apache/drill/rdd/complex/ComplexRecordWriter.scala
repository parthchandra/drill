package org.apache.drill.rdd.complex

import java.util

import io.netty.buffer.DrillBuf
import org.apache.drill.exec.expr.TypeHelper
import org.apache.drill.exec.expr.holders.{Float8Holder, BitHolder, Decimal18Holder, VarCharHolder}
import org.apache.drill.exec.memory.{BufferAllocator, TopLevelAllocator}
import org.apache.drill.exec.physical.impl.OutputMutator
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery
import org.apache.drill.exec.record.{FragmentWritableBatch, ExtendedFragmentWritableBatch, WritableBatch, MaterializedField}
import org.apache.drill.exec.vector.ValueVector
import org.apache.drill.exec.vector.complex.MapVector
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter
import org.apache.drill.exec.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.drill.exec.vector.complex.writer._
import org.apache.drill.rdd.{SendingDrillPartition, DrillPartition, DrillOutgoingRowType}
import org.apache.drill.rdd.complex.query.ExtendedDrillClient
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Try}

class ComplexRecordWriter[T<:DrillOutgoingRowType](containerWriter: VectorContainerWriter, factory: HolderFactory) {
  private var count = 0

  def vector = containerWriter.getMapVector

  protected def write(writer:BaseWriter, value:CValue): Unit = {
    value match {
      case CNothing =>
      // todo: how do you write nothing?
      case CNull =>
      // todo: how do you write null?
      case CString(value) => {
        val holder  = factory.createVarCharHolder(value)
        writer.asInstanceOf[VarCharWriter].write(holder)
        holder.buffer.release()
      }
      case CNumber(value:BigDecimal) => {
        val holder = new Float8Holder
        holder.value = value.doubleValue
        writer.asInstanceOf[Float8Writer].write(holder)
      }
      case CBool(value) => {
        val holder = new BitHolder
        holder.value = value match {
          case true => 1
          case _ => 0
        }
        writer.asInstanceOf[BitWriter].write(holder)
      }
      case CArray(values) => {
        val listWriter = writer.asInstanceOf[ListWriter]
        listWriter.start()
        values.foreach {
          case value:CObject =>
            write(listWriter.map(), value)
          case value:CArray =>
            write(listWriter.list(), value)
          case value:CString =>
            write(listWriter.varChar(), value)
          case value:CNumber =>
            write(listWriter.float8(), value)
          case value:CBool =>
            write(listWriter.bit(), value)
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
            write(mapWriter.map(name), value)
          case (name, value:CArray) =>
            write(mapWriter.list(name), value)
          case (name, value:CString) =>
            write(mapWriter.varChar(name), value)
          case (name, value:CNumber) =>
            write(mapWriter.float8(name), value)
          case (name, value:CBool) =>
            write(mapWriter.bit(name), value)
          case _ =>
          //TODO: how do we represent nothing, null?
        }
        mapWriter.end()
      }
    }
  }

  def write(record:T): Unit = {
    write(containerWriter.rootAsMap, record)
    count += 1
    containerWriter.setValueCount(count)
    containerWriter.setPosition(count)
  }

  def close(): Unit = {
    containerWriter.clear()
    vector.clear()
  }
}

object ComplexRecordWriter {
  def apply[T<:DrillOutgoingRowType](allocator: BufferAllocator) = {
    val mutator = new CachingMutator(allocator)
    val containerWriter = new VectorContainerWriter(mutator)
    val factory = new HolderFactory(allocator)
    new ComplexRecordWriter[T](containerWriter, factory)
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
  val vectors = new mutable.HashMap[MaterializedField, ValueVector]
  val logger = LoggerFactory.getLogger(getClass)

  override def addField[T <: ValueVector](field: MaterializedField, clazz: Class[T]): T = {
    vectors.getOrElseUpdate(field, TypeHelper.getNewVector(field, allocator)).asInstanceOf[T]
  }

  override def addFields(vvList: util.List[ValueVector]): Unit = ???
  override def allocate(recordCount: Int): Unit = ???
  override def isNewSchema: Boolean = ???
  override def getManagedBuffer: DrillBuf = ???
}


class VectorUploader(partition: SendingDrillPartition, allocator:BufferAllocator) {
  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val query = QueryFragmentQuery.newBuilder()
    .addAllFragments(partition.drill.queryPlan.getFragmentsList)
    .setFragmentHandle(partition.drill.fragment.getHandle)
    .build()

  protected lazy val client:Try[ExtendedDrillClient] = {
    val client = new ExtendedDrillClient(allocator)
    val address = Some(partition.drill.fragment.getAssignment)
    client.connect(address).flatMap(_ => Try(client))
  }

  def upload(vector:MapVector, isLast:Boolean=false):Try[Unit] = {
    val handle = partition.drill.fragment.getHandle
    val writableBatch = WritableBatch.getBatchNoHV(vector.getAccessor.getValueCount, vector, false)
    val fWritableBatch = new FragmentWritableBatch(isLast, handle.getQueryId, -1, -1, handle.getMajorFragmentId, handle.getMinorFragmentId, writableBatch)
    val extendedBatch = new ExtendedFragmentWritableBatch(query, fWritableBatch)
    client.flatMap(c => c.upload(extendedBatch))
  }

  def close():Unit = client.foreach(c=>c.close())

}
