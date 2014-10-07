package org.apache.drill.rdd

import org.apache.drill.common.types.TypeProtos.{DataMode, MajorType, MinorType}
import org.apache.drill.exec.proto.ExecProtos.PlanFragment
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments
import org.apache.drill.exec.store.spark.RDDTableSpec
import org.apache.drill.rdd.DrillConversions._
import org.apache.drill.rdd.complex.{ComplexRecordWriter, SqlAnalyzer}
import org.apache.drill.rdd.complex.query.ExtendedDrillClient
import org.apache.drill.rdd.resource.{using}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try


class RDDRegistry[OUT<:DrillOutgoingRowType:ClassTag] extends Iterable[(String, RDD[OUT])] with Serializable {
  private val name2rdd = mutable.Map[String, RDD[OUT]]()

  def register(name:String, rdd:RDD[OUT]): Unit = {
    name2rdd += (name.toUpperCase -> rdd)
  }

  def unregister(name:String): Unit = {
    name2rdd -= name.toUpperCase
  }

  def find(name:String): Option[RDD[OUT]] = {
    name2rdd.get(name.toUpperCase)
  }

  def iterator = name2rdd.iterator
  def names = name2rdd.keys
  def rdds = name2rdd.values
}

case class DrillContext[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sql:String,
                                                managerFactory:QueryManagerFactoryType[IN],
                                                registry:RDDRegistry[OUT],
                                                recevingRDD:Boolean)

case class DrillPartition(val queryPlan:QueryPlanFragments, val index:Int) extends Partition with Serializable {
  def fragment:PlanFragment = queryPlan.getFragments(index)
  override def toString = s"index: $index - fragments: $queryPlan"
}


class DrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](@transient sc:SparkContext, dc:DrillContext[IN, OUT])
  extends RDD[IN](sc, Nil) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val analyzer = new SqlAnalyzer(dc.sql, Set() ++ dc.registry.names)

  protected lazy val delegate:RDD[IN] = {
    val delegateCtx = Try {
      analyzer.analyze match {
        case names if names!=null && names.length>0 =>
          val registry = names.toList.foldLeft(new RDDRegistry[OUT]()) {
            case (registry, name) =>
              sc.getRegistry.find(name) match {
                case Some(rdd) => registry.register(name, rdd.asInstanceOf[RDD[OUT]])
                case None => throw new IllegalStateException("Unable to find RDD in registry. Should never reach here.")
              }
              registry
          }
          val specs = registry.map { case (n, rdd) => (n, new RDDTableSpec(n, rdd.partitions.length)) }.toMap
          val expandedSql = analyzer.expand(specs)
          DrillContext(expandedSql, dc.managerFactory, registry, false)
        case _ => dc
      }
    }.get

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
      case true => new ReceivingDrillRDD[IN, OUT](sc, dc)
      case false => new SendingDrillRDD[IN, OUT](sc, dc)
    }
  }
}

class ReceivingDrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
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

class SendingDrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
  extends ReceivingDrillRDD[IN, OUT](sc, dc) {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def createPushManager(split:Partition) = {
    (it:Iterator[OUT]) => {
      using(new ComplexRecordWriter[OUT]) { w =>
        while (it.hasNext) {
          w.write(it.next)
        }
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[IN] = {
    val it = super.compute(split, context)
    dc.registry.map {
      case (name, rdd) => sc.runJob(rdd, createPushManager(split))
    }
    it
  }
}