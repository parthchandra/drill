package org.apache.drill.rdd.query

import org.apache.drill.exec.inputformat.StreamingBatchListener
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.ValueVector
import org.apache.drill.exec.vector.complex.MapVector
import org.apache.drill.exec.vector.complex.impl.CombinedMapVector
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait RecordIterator[T] extends Iterator[T]

/**
 * A record iterator that works on streaming {{RecordBatch}}es
 *
 * @param loader record batch loader
 * @param listener streaming batch listener
 * @param recordFactory creates an instance of record
 * @tparam T record type
 */
class StreamingRecordIterator[T:ClassTag](loader: RecordBatchLoader,
                                 listener: StreamingBatchListener,
                                 recordFactory: (MapVector, Int)=>T) extends RecordIterator[T] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var delegate:Iterator[T] = null
  private val vector = new CombinedMapVector(loader)

  override def hasNext: Boolean = {
    if (delegate == null || !delegate.hasNext) {
      delegate = Try(listener.getNext)
        .flatMap { qrb =>
          Try(loader.load(qrb.getHeader.getDef, qrb.getData))
            .flatMap(_ => Try(loader))
      } flatMap { loader =>
          //TODO: handle schema changes?
          vector.load()
          val rowCount = loader.getRecordCount
          Try((0 until rowCount) map(row=>recordFactory(vector, row)) iterator)
        } match {
          case Success(it)=>it
          case Failure(t)=>
            if (!t.isInstanceOf[NoSuchElementException]) {
              logger.error("Error while creating the iterator", t)
            }
            Array[T]().iterator
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
