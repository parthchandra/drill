package org.apache.drill.rdd

import java.util

import io.netty.buffer.DrillBuf
import org.apache.drill.exec.expr.TypeHelper
import org.apache.drill.exec.memory.BufferAllocator
import org.apache.drill.exec.physical.impl.OutputMutator
import org.apache.drill.exec.record.MaterializedField
import org.apache.drill.exec.vector.ValueVector
import org.apache.drill.rdd.complex._
import org.apache.drill.rdd.resource._

import scala.collection.mutable

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

object WriterTest extends App {

//  val allocator = new TopLevelAllocator(DrillConfig.createClient())
//  val mutator = new CachingMutator(allocator)
//  val writer = new VectorContainerWriter(mutator)
//
//  writer.rootAsMap().start()
//  val ageHolder = new Decimal18Holder
//  ageHolder.value = 29
//  writer.rootAsMap().decimal18("age").write(ageHolder)
//
//  val nameHolder = new VarCharHolder
//  val name = "hanifi".getBytes
//  nameHolder.buffer = allocator.buffer(name.length)
//  nameHolder.buffer.setBytes(0, name)
//  nameHolder.start = 0
//  nameHolder.end = name.length
//  writer.rootAsMap().varChar("name").write(nameHolder)
//  nameHolder.buffer.release()
//  writer.rootAsMap().end()
//
//  val vector = writer.getMapVector
//  println(vector.getAccessor.getObject(0))
//  writer.clear()
//  allocator.close()

  using(new ComplexRecordWriter[DrillOutgoingRowType]) { w=>
    val baseAge = 30
    (0 until 10).foreach {i =>
      val info = CObject(("hobbies" , CArray(CString("Reading"), CString("Swimming"))))
      val record = CObject(("name", CString("hanifi")), ("age", CNumber(baseAge+i)), ("info", info))
      w.write(record)
    }

    (0 until w.vector.getAccessor.getValueCount).foreach { i =>
      println(w.vector.getAccessor.getObject(i))
    }
  }


}
