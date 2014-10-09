package org.apache.drill.rdd.complex

import java.nio.CharBuffer
import java.nio.charset.{Charset, CharsetEncoder}

import io.netty.buffer.{ByteBuf, DrillBuf}
import org.apache.drill.exec.expr.holders.{BitHolder, VarCharHolder, Float8Holder}
import org.apache.drill.exec.memory.BufferAllocator
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter
import org.apache.drill.exec.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.drill.exec.vector.complex.writer.{VarCharWriter, BitWriter, Float8Writer}
import org.apache.drill.rdd.{GenericMatcher, DrillOutgoingRowType}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar

import org.apache.drill.rdd.complex.WritableRecordConversions._

class ComplexRecordWriterSpec extends FlatSpec with Matchers with MockitoSugar {

  // set-up record writer
  val containerWriter = mock[VectorContainerWriter]
  val allocator = mock[BufferAllocator]
  val recordWriter = new ComplexRecordWriter[DrillOutgoingRowType](allocator, containerWriter)

  val rootMapWriter = mock[MapWriter]
  when(containerWriter.rootAsMap()).thenReturn(rootMapWriter)

  "A complex writer" should "increment record count after a write" in {
    val emptyRecord = CObject()
    recordWriter.write(emptyRecord)
    recordWriter.count should be (1)

    verify(rootMapWriter).start()
    verify(rootMapWriter).end()
  }

  it should "write inner map into writer" in {
    val singleNestedMapRecord = CObject(("inner", CObject()))
    val innerMapWriter = mock[MapWriter]
    reset(rootMapWriter)
    when(rootMapWriter.map("inner")).thenReturn(innerMapWriter)

    recordWriter.write(singleNestedMapRecord)

    verify(rootMapWriter).start()
    verify(innerMapWriter).start()
    verify(innerMapWriter).end()
    verify(rootMapWriter).end()
  }


  it should "write inner array into writer" in {
    val singleNestedMapRecord = CObject(("inner", CArray()))
    val innerListWriter = mock[ListWriter]
    reset(rootMapWriter)
    when(rootMapWriter.list("inner")).thenReturn(innerListWriter)

    recordWriter.write(singleNestedMapRecord)

    verify(rootMapWriter).start()
    verify(innerListWriter).start()
    verify(innerListWriter).end()
    verify(rootMapWriter).end()
  }

  it should "write objects recursively into writer" in {
    val doubleNestedMapRecord = CObject(("inner", CArray(CObject())))
    val innerListWriter = mock[ListWriter]
    val levelTwoMapWriter = mock[MapWriter]

    reset(rootMapWriter)
    when(rootMapWriter.list("inner")).thenReturn(innerListWriter)
    when(innerListWriter.map()).thenReturn(levelTwoMapWriter)

    recordWriter.write(doubleNestedMapRecord)

    verify(rootMapWriter).start()
    verify(innerListWriter).start()
    verify(levelTwoMapWriter).start()
    verify(levelTwoMapWriter).end()
    verify(innerListWriter).end()
    verify(rootMapWriter).end()
  }

  it should "write number, bool into writer" in {
    val text = "text"
    val numberRecord = CObject(
      ("innerNumber", 10),
      ("innerBool", false)
    )
    val innerFloat8Writer = mock[Float8Writer]
    val innerBitWriter = mock[BitWriter]

    reset(rootMapWriter)
    when(rootMapWriter.float8("innerNumber")).thenReturn(innerFloat8Writer)
    when(rootMapWriter.bit("innerBool")).thenReturn(innerBitWriter)

    recordWriter.write(numberRecord)

    verify(rootMapWriter).start()
    verify(innerFloat8Writer).write(argThat(new GenericMatcher[Float8Holder] {
      override def matches(item: scala.Any): Boolean = {
        item.asInstanceOf[Float8Holder] match {
          case h:Float8Holder => h.value == 10;
          case _ => false
        }
      }
    }))

    verify(innerBitWriter).write(argThat(new GenericMatcher[BitHolder] {
      override def matches(item: scala.Any): Boolean = {
        item.asInstanceOf[BitHolder] match {
          case h:BitHolder => h.value == 0;
          case _ => false
        }
      }
    }))

    verify(rootMapWriter).end()
  }

}
