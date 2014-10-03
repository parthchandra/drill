package org.apache.drill.spark.sql

import org.apache.drill.exec.proto.ExecProtos.PlanFragment
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments
import org.apache.drill.exec.store.spark.RDDTableSpec
import org.apache.drill.spark.{DrillOutgoingRowType, QueryManagerFactoryType}
import org.apache.drill.spark.rdd.DrillConversions
import DrillConversions._
import org.apache.drill.spark.sql.query.{ExtendedDrillClient}
import scala.collection.JavaConversions._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.Try


case class DrillContext[IN:ClassTag, OUT:ClassTag](sql:String,
                                                managerFactory:QueryManagerFactoryType[IN],
                                                registry:RDDRegistry[OUT],
                                                recevingRDD:Boolean)

case class DrillPartition(val queryPlan:QueryPlanFragments, val index:Int) extends Partition with Serializable {
  def fragment:PlanFragment = queryPlan.getFragments(index)
  override def toString = s"index: $index - fragments: $queryPlan"
}


class DrillRDD[IN:ClassTag, OUT:ClassTag](@transient sc:SparkContext, dc:DrillContext[IN, OUT]) extends RDD[IN](sc, Nil) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val analyzer = new SqlAnalyzer(dc.sql, Set() ++ dc.registry.names)

  protected lazy val delegate:RDD[IN] = {
    var delegateCtx:DrillContext[IN, OUT] = dc
    if (analyzer.needsSqlExpansion()) {
      throw new UnsupportedOperationException
      val spec = null
//      analyzer.getExpandedSql();
      val names:Seq[String] = null; //analyzer.getExpandedSql(Map());
      val registry = names.foldLeft(new RDDRegistry[OUT]()) {
        case (registry, name) =>
          sc.getRegistry.find(name) match {
            case Some(rdd) => registry.register(name, rdd.asInstanceOf[RDD[OUT]])
            case None => throw new IllegalStateException("Unable to find RDD in registry. Should never reach here.")
          }
          registry
      }
      val expandedSql = dc.sql
      delegateCtx = DrillContext(expandedSql, dc.managerFactory, registry, false)
    }
    createDelegate(sc, delegateCtx)
  }

  override protected def getPartitions: Array[Partition] = {
    delegate.partitions
  }

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[IN] = {
    delegate.compute(split, task)
  }

  protected def createDelegate(sc: SparkContext, dc: DrillContext[IN, OUT]): RDD[IN] = {
    dc.recevingRDD match {
      case true => new ReceivingRDD[IN, OUT](sc, dc)
      case false => new SendingRDD[IN, OUT](sc, dc)
    }
  }
}

class ReceivingRDD[IN:ClassTag, OUT:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
  extends RDD[IN](sc, Nil) {
  private val logger = LoggerFactory.getLogger(getClass)

  override protected def getPartitions: Array[Partition] = {
    val client = new ExtendedDrillClient
    val partitions = client.connect()
      .flatMap(c=>c.getPlanFor(dc.sql))
      .map(p=>(0 until p.getFragmentsCount).map(i=>DrillPartition(p, i)))
    client.close()
    partitions.get.toArray
  }

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[IN] = {
    val drillPartition = split.asInstanceOf[DrillPartition]
    val manager = dc.managerFactory()
    task.addTaskCompletionListener { _ =>
      logger.info("closing query manager")
      manager.close()
    }

    Try(manager.execute(drillPartition))
      .map (it=> new InterruptibleIterator[IN](task, it))
      .get
  }
}

class SendingRDD[IN:ClassTag, OUT:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
  extends RDD[IN](sc, Nil) {
  private val logger = LoggerFactory.getLogger(getClass)

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[IN] = ???

  override protected def getPartitions: Array[Partition] = ???
}