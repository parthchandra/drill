package org.apache.drill.rdd.query

import org.apache.drill.exec.inputformat.StreamingListener
import org.apache.drill.exec.memory.{BufferAllocator, TopLevelAllocator}
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery
import org.apache.drill.rdd._

import scala.util.Try

trait QueryEngine[T>:SparkRowType] {

  def run(partition:DrillPartition): Iterator[T]

  def close(): Unit
}

class StreamingQueryEngine[T>:SparkRowType](allocator: BufferAllocator) extends QueryEngine[T] {

  val client = new ExtendedDrillClient()

  override def run(partition: DrillPartition): Iterator[T] = {
    val query = QueryFragmentQuery.newBuilder()
      .addAllFragments(partition.queryPlan.getFragmentsList)
      .setFragmentHandle(partition.fragment.getHandle)
      .build()

    val listener = new StreamingListener
    val endpoint = partition.fragment.getAssignment
    client.connect(Some(endpoint))
      .flatMap(c=>Try(c.getFragment(query, listener)))
      .get

    new StreamingIterator[T](allocator, query.getFragmentHandle, listener)
  }

  override def close() = client.close()
}