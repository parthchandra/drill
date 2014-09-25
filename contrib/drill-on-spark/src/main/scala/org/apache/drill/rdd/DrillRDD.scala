package org.apache.drill.rdd

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.{TopLevelAllocator, BufferAllocator}
import org.apache.drill.exec.proto.ExecProtos.PlanFragment
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments
import org.apache.drill.rdd.DrillConversions._
import org.apache.drill.rdd.query.{ExtendedDrillClient, QueryManager}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Try

class DrillRDD[T:ClassTag](sc:SparkContext, sql:String,
               managerFactory: () => QueryManager[T],
               timeout:Duration=10 seconds)
  extends RDD[T](sc, Nil) {

//  private val config = DrillConfig.createClient
//  private val allocatorRef:Option[BufferAllocator] = None
//
//  private val allocator:Option[BufferAllocator] = allocatorRef match {
//    case None => Some(new TopLevelAllocator(config))
//    case a:Some[BufferAllocator] => a
//  }

  private val logger = LoggerFactory.getLogger(getClass)

  override protected def getPartitions: Array[Partition] = {
    val client = new ExtendedDrillClient
    val partitions = client.connect()
      .flatMap(c=>c.getPlanFor(sql))
      .map(p=>(0 until p.getFragmentsCount).map(i=>DrillPartition(p, i)))
    client.close()
    partitions.get.toArray
  }

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[T] = {
    val drillPartition = split.asInstanceOf[DrillPartition]
    val manager = managerFactory()
    task.addTaskCompletionListener { _ =>
      logger.info("closing query manager")
      manager.close()
    }

    Try(manager.execute(drillPartition))
      .map (it=> new InterruptibleIterator[T](task, it))
      .get
  }
}

case class DrillPartition(val queryPlan:QueryPlanFragments, val index:Int) extends Partition with Serializable {
  def fragment:PlanFragment = queryPlan.getFragments(index)
  override def toString = s"index: $index - fragments: $queryPlan"
}

