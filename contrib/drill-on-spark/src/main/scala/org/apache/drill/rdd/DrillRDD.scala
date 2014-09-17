package org.apache.drill.rdd

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.exec.proto.ExecProtos.PlanFragment
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments
import org.apache.drill.rdd.query.{ExtendedDrillClient, StreamingQueryEngine, QueryEngine}
import DrillConversions._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.concurrent.duration.Duration
import scala.util.Try

import DrillConversions._

class DrillRDD(sc:SparkContext, sql:String, engineFactory: () => QueryEngine[SparkRowType], timeout:Duration=10 seconds) extends RDD[SparkRowType](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val client = new ExtendedDrillClient()
    val partitions = client.connect()
      .flatMap(c=>c.getPlanFor(sql))
      .flatMap(p=>Try((0 until p.getFragmentsCount).map(i=>DrillPartition(p, i))))
    client.close()
    partitions.get.toArray
  }

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[SparkRowType] = {
    val engine = engineFactory()
    val drillPartition = split.asInstanceOf[DrillPartition]
    task.addTaskCompletionListener { _ => engine.close() }

    Try(engine.run(drillPartition))
      .map (it=> new InterruptibleIterator[SparkRowType](task, it))
      .get
  }
}

case class DrillPartition(val queryPlan:QueryPlanFragments, val index:Int) extends Partition with Serializable {

  def fragment:PlanFragment = queryPlan.getFragments(index)

  override def toString = s"index: $index - fragments: $queryPlan"
}

