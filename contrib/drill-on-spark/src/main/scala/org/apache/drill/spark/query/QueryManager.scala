package org.apache.drill.spark.sql.query

import org.apache.drill.exec.inputformat.StreamingBatchListener
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.complex.MapVector
import org.apache.drill.spark.sql._
import org.apache.drill.spark.sql.sql.RecordInfo
import org.apache.drill.spark.RecordFactoryType
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.{Success, Failure, Try}

trait QueryManager[T] {

  def execute(partition:DrillPartition): Iterator[T]

  def close(): Unit
}

case class QueryContext[IN:ClassTag](loader: RecordBatchLoader, recordFactory:RecordFactoryType[IN])

class StreamingQueryManager[T:ClassTag](ctx:QueryContext[T]) extends QueryManager[T] {

  private val logger = LoggerFactory.getLogger(getClass)
  private val empty = Array[T]().iterator
  private val client = new ExtendedDrillClient(ctx.loader.getAllocator)

  override def execute(partition: DrillPartition): Iterator[T] = {
    val query = QueryFragmentQuery.newBuilder()
      .addAllFragments(partition.queryPlan.getFragmentsList)
      .setFragmentHandle(partition.fragment.getHandle)
      .build()

    logger.info("executing partition[{}] fragment[{}]", partition.index, partition.fragment)

    val listener = new StreamingBatchListener
    val endpoint = partition.fragment.getAssignment
    client.connect(Some(endpoint))
      .map(c=>c.getFragment(query, listener)) match {
        case Failure(t) =>
          logger.error("unable to get fragment", t)
          empty
        case Success(_) =>
          new StreamingRecordIterator[T](ctx, listener)
    }
  }

  override def close() = client.close()
}