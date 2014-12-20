package org.apache.drill.rdd

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.memory.TopLevelAllocator
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments
import org.apache.drill.exec.store.spark.{SparkSubScan, RDDTableSpec}
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter
import org.apache.drill.rdd.DrillConversions._
import org.apache.drill.rdd.complex._
import org.apache.drill.rdd.complex.query.{StreamingQueryManager, ExtendedDrillClient}
import org.apache.drill.rdd.resource.{using}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
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

case class DrillContext[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sql:String, registry:RDDRegistry[OUT],
                                                                         inbound:Boolean)

case class DrillPartition(index:Int, queryPlan:QueryPlanFragments) extends Partition with Serializable {
  val fragment = queryPlan.getFragments(index)
  val fragmentHandle = fragment.getHandle
  override def toString = s"index: $index - fragment handle: $fragmentHandle"
}

case class RDDDefinition(name:String, partitions:Seq[Partition])

case class SendingDrillPartition(index:Int, last:Boolean, drill: DrillPartition, parent:RDDDefinition) extends Partition with Serializable

class DrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](@transient sc:SparkContext, dc:DrillContext[IN, OUT])
  extends RDD[IN](sc, Nil) {

  private val logger = LoggerFactory.getLogger(getClass)

  protected lazy val drillCtx: DrillContext[IN, OUT] = {
    val analyzer = new SqlAnalyzer(dc.sql, Set() ++ dc.registry.names)
    analyzer.analyze match {
      case names if names!=null && names.length>0 =>
        val registry = new RDDRegistry[OUT]
        for (name <- names) {
          val rdd = sc.getRegistry.find(name).getOrElse {
            throw new IllegalStateException("Unable to find RDD in registry. Should never reach here.")
          }
          registry.register(name, rdd.asInstanceOf[RDD[OUT]])
        }
        val specs = registry.map { case (n, rdd) => (n, new RDDTableSpec(n, rdd.partitions.length)) }.toMap
        val expandedSql = analyzer.expand(specs)
        DrillContext(expandedSql, registry, false)
      case _ => dc
    }
  }

  protected lazy val delegate: RDD[IN] = createDelegate(sc, drillCtx)

  override protected def getPartitions: Array[Partition] = delegate.partitions

  override protected def getDependencies: Seq[Dependency[_]] = delegate.dependencies

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[IN] = delegate.compute(split, task)

  protected def createDelegate(sc: SparkContext, dc: DrillContext[IN, OUT]): RDD[IN] = {
    if (dc.inbound) {
      new ReceivingDrillRDD[IN, OUT](sc, dc)
    } else {
      new SendingDrillRDD[IN, OUT](sc, dc)
    }
  }
}

class SubScanSpecMapper[T](cls:Class[T]) {
  val mapper = new ObjectMapper()
  mapper.registerSubtypes(new NamedType(cls))

  def read(partition:DrillPartition):Option[T] = {
    val fragment = mapper.readTree(partition.fragment.getFragmentJson)
    val subScanNode = fragment.findParent("tableSpec")
    mapper.readValue(subScanNode.toString, cls) match {
      case null => None
      case scan:T => Some(scan)
    }
  }
}

class ReceivingDrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
  extends RDD[IN](sc, Nil) {
  private val logger = LoggerFactory.getLogger(getClass)

  protected val recordFactory = (info:FieldReaderInfo) => new DrillReadableRecord(info).asInstanceOf[IN]

  override protected def getPartitions: Array[Partition] = {
    val client = new ExtendedDrillClient
    val partitions = client.connect()
      .flatMap(c=>c.getPlanFor(dc.sql))
      .map(p=>(0 until p.getFragmentsCount)
      .map(i=>DrillPartition(i, p)))
    client.close()
    partitions.get.toArray
  }

  @DeveloperApi
  override def compute(split: Partition, task: TaskContext): Iterator[IN] = {
    val drillPartition = split.asInstanceOf[DrillPartition]
    val config = DrillConfig.createClient()
    val allocator = new TopLevelAllocator(config)
    val manager = StreamingQueryManager[IN](allocator, recordFactory)

    Try(manager.execute(drillPartition))
      .map (it=> new InterruptibleIterator[IN](task, it))
      .get
  }
}

class SendingDrillRDD[IN:ClassTag, OUT<:DrillOutgoingRowType:ClassTag](sc:SparkContext, dc:DrillContext[IN, OUT])
  extends ReceivingDrillRDD[IN, OUT](sc, dc) {
  private val logger = LoggerFactory.getLogger(getClass)

  override protected def getPartitions: Array[Partition] = {
    val partitions = super.getPartitions.map(p => p.asInstanceOf[DrillPartition])
    val mapper = new SubScanSpecMapper(classOf[SparkSubScan.SparkSubScanSpec])
    partitions.map { partition =>
      mapper.read(partition).map { subScan =>
        val name = subScan.getTableSpec.getName
        val parentPartitions = dc.registry.find(name).map(_.partitions).getOrElse(Array[Partition]())
        SendingDrillPartition(partition.index, partition.index==partitions.length-1, partition, RDDDefinition(name, parentPartitions))
      }.getOrElse {
        throw new IllegalStateException(s"Unable to read subscan definition from drill partition: $partition")
      }
    }
  }

  override protected def getDependencies: Seq[Dependency[_]] = {
    val dependencies = new mutable.ArrayBuffer[Dependency[_]]
    var pos = 0
    for (partition <- partitions) {
      val name = partition.asInstanceOf[SendingDrillPartition].parent.name
      val parent = dc.registry.find(name).getOrElse {
        throw new IllegalStateException(s"Unable to find registered rdd $name")
      }
      //TODO: define dependencies based on assigments
      dependencies += new RangeDependency(parent, 0, pos, parent.partitions.length)
      pos += parent.partitions.length
    }
    dependencies
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[IN] = {
    val partition = split.asInstanceOf[SendingDrillPartition]
    val config = DrillConfig.createClient()
    using(new TopLevelAllocator(config)) { allocator =>
      using(new VectorUploader(partition, allocator)) { uploader =>
        val parent = dependencies(partition.index)
        for (parentPartition <- partition.parent.partitions) {
          logger.debug(s"pushing parent partition $parentPartition")
          using(ComplexRecordWriter[DrillOutgoingRowType](allocator)) { writer =>
            val results = parent.asInstanceOf[RangeDependency[OUT]].rdd.iterator(parentPartition, context)
            while (results.hasNext) {
              writer.write(results.next)
            }
            uploader.upload(writer.vector, partition.last)
          }
        }
      }
    }
    logger.debug("done pushing")
    super.compute(partition.drill, context)
  }

}