package org.apache.drill.rdd.query

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.common.exceptions.DrillRuntimeException
import org.apache.drill.exec.inputformat.StreamingListener
import org.apache.drill.exec.memory.{BufferAllocator, TopLevelAllocator}
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle
import org.apache.drill.exec.record.{RecordBatchLoader, RecordBatch}
import org.apache.drill.rdd.SparkRowType
import org.apache.drill.rdd.sql.{DrillRow, NamedRow}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

trait RecordIterator[T>:SparkRowType] extends Iterator[T]

class StreamingIterator[T>:SparkRowType](allocator: BufferAllocator,
                                    fragment:FragmentHandle,
                                    listener:StreamingListener) extends RecordIterator[T] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var delegate:Iterator[T] = null

  override def hasNext: Boolean = {
    val hasNext = listener.hasNext
    if (!hasNext) return false

    if (delegate == null || !delegate.hasNext) {
      val loader = new RecordBatchLoader(allocator)
      delegate = Try(listener.getNext)
        .flatMap { qrb =>
        //TODO: handle schema changes?
        Try(loader.load(qrb.getHeader.getDef, qrb.getData))
          .recover { case t:NullPointerException => loader.clear()}
          .flatMap(_ => Try(loader))
      } flatMap { loader =>
        val rowCount = loader.getRecordCount
        Try((0 until rowCount) map(row=>new DrillRow(loader, row)) iterator)
      } match {
        case Success(it)=>it
        case Failure(t)=>logger.error("Done with the query", t); Array[DrillRow]().iterator
      }
    }

    if (!delegate.hasNext) {
      listener.close()
    }
    delegate.hasNext
  }

  override def next(): T = {
    delegate.next
  }
}
