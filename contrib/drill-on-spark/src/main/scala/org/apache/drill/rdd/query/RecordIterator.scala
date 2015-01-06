package org.apache.drill.rdd.complex.query

import org.apache.drill.exec.inputformat.StreamingBatchListener
import org.apache.drill.exec.vector.complex.impl.CombinedMapVector

import org.apache.drill.rdd.complex.Backend
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait RecordIterator[T] extends Iterator[T]

/**
 * A record iterator that works on streaming {{RecordBatch}}es
 *
 * @param ctx query context
 * @param listener streaming batch listener
 * @tparam T record type
 */
class StreamingRecordIterator[T:ClassTag](ctx:QueryContext[T], listener: StreamingBatchListener, whenConsumed: ()=>Unit)
  extends RecordIterator[T] {

  private val logger = LoggerFactory.getLogger(getClass)
  private var delegate:Iterator[T] = null
  private val vector = new CombinedMapVector(ctx.loader)
  private val empty = Array[T]().iterator
  var batch = 0
  var loaded = false

  override def hasNext: Boolean = {
    if (delegate == null || !delegate.hasNext) {
      delegate = Try(listener.getNext)
        .flatMap { qrb =>
          Try {
            if (qrb == null) {
              ctx.loader.clear()
            } else {
              ctx.loader.load(qrb.getHeader.getDef, qrb.getData)
              vector.load()
            }
            val rowCount = ctx.loader.getRecordCount
            logger.info(s"loader got $rowCount records")
            batch += 1
            (0 until rowCount) map {
              row => ctx.recordFactory(Backend(vector.getAccessor.getReader, row))
            } iterator
          }
        } match {
          case Success(it) => it
          case Failure(t) =>
            logger.error("error while iterating over record batches", t)
            empty
        }
    }

    if (!delegate.hasNext) {
      whenConsumed()
    }
    delegate.hasNext
  }

  override def next(): T = {
    delegate.next
  }
}
