package org.apache.drill.rdd.query

import org.apache.drill.exec.inputformat.StreamingBatchListener
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery
import org.apache.drill.exec.record.RecordBatchLoader
import org.apache.drill.exec.vector.complex.MapVector
import org.apache.drill.rdd._

import scala.reflect.ClassTag
import scala.util.Try

trait QueryManager[T] {

  def execute(partition:DrillPartition): Iterator[T]

  def close(): Unit
}

class StreamingQueryManager[T:ClassTag](loader: RecordBatchLoader,
                                        recordFactory:(MapVector, Int)=>T)
  extends QueryManager[T] {

  val client = new ExtendedDrillClient()

  override def execute(task: DrillPartition): Iterator[T] = {
    val query = QueryFragmentQuery.newBuilder()
      .addAllFragments(task.queryPlan.getFragmentsList)
      .setFragmentHandle(task.fragment.getHandle)
      .build()

    val listener = new StreamingBatchListener
    val endpoint = task.fragment.getAssignment
    client.connect(Some(endpoint))
      .flatMap(c=>Try(c.getFragment(query, listener)))
      .get

    new StreamingRecordIterator[T](loader, listener, recordFactory)
  }

  override def close() = client.close()
}