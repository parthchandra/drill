package org.apache.drill.rdd.complex.query

import org.apache.drill.exec.inputformat.StreamingBatchListener
import org.apache.drill.exec.memory.BufferAllocator
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery
import org.apache.drill.exec.record.RecordBatchLoaderFactory
import org.apache.drill.rdd.{resource, DrillPartition, RecordFactoryType}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

trait QueryManager[T] {

  def execute(partition:DrillPartition): Iterator[T]

  def close(): Unit
}

case class QueryContext[IN:ClassTag](loaderFactory: RecordBatchLoaderFactory, recordFactory:RecordFactoryType[IN])

class StreamingQueryManager[T:ClassTag](ctx:QueryContext[T]) extends QueryManager[T] {

  private val logger = LoggerFactory.getLogger(getClass)
  private val empty = Array[T]().iterator

  override def execute(partition: DrillPartition): Iterator[T] = {
    val query = QueryFragmentQuery.newBuilder()
      .addAllFragments(partition.queryPlan.getFragmentsList)
      .setFragmentHandle(partition.fragment.getHandle)
      .build()

    logger.debug(s"querying drill partition: $partition")

    val listener = new StreamingBatchListener
    val endpoint = partition.fragment.getAssignment
    val client = new ExtendedDrillClient(ctx.loaderFactory.getAllocator)
    client.connect(Some(endpoint)).map(c => c.getFragment(query, listener)) match {
      case Failure(t) =>
        logger.error("unable to get fragment", t)
        empty
      case Success(_) =>
        new StreamingRecordIterator[T](ctx, listener, () => {
          listener.close()
          client.close()
        })
    }
  }

  override def close():Unit = {}
}

object StreamingQueryManager {
  def apply[IN:ClassTag](allocator: BufferAllocator, factory: RecordFactoryType[IN]) = {
    val loaderFactory = new RecordBatchLoaderFactory(allocator)
    new StreamingQueryManager[IN](QueryContext(loaderFactory, factory))
  }
}