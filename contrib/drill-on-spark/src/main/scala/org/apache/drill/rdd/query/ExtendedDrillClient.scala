package org.apache.drill.rdd.complex.query

import org.apache.drill.common.config.DrillConfig
import org.apache.drill.exec.client.DrillClient
import org.apache.drill.exec.memory.BufferAllocator
import org.apache.drill.exec.proto.CoordinationProtos
import org.apache.drill.exec.proto.UserProtos.{QueryFragmentQuery, QueryPlanFragments}
import org.apache.drill.exec.record.ExtendedFragmentWritableBatch
import org.apache.drill.exec.rpc.user.UserResultsListener

import scala.util.Try


class ExtendedDrillClient(config:DrillConfig, allocator:BufferAllocator) {

  def this(allocator:BufferAllocator) = {
    this(DrillConfig.createClient(), allocator)
  }

  def this() = {
    this(null)
  }

  private val client = new DrillClient(config, null, allocator)

  def connect(endpoint:Option[CoordinationProtos.DrillbitEndpoint]=None):Try[ExtendedDrillClient] = {
    Try(
      endpoint match {
        case Some(addr) => client.connect(addr); this
        case None => client.connect(); this
      }
    )
  }

  def close():Unit = client.close()

  def getPlanFor(sql:String):Try[QueryPlanFragments] = Try(client.planQuery(sql).get)

  def getFragment(query:QueryFragmentQuery, listener:UserResultsListener) =
    Try(client.submitReadFragmentRequest(query, listener))

  def upload(batch:ExtendedFragmentWritableBatch) = Try(client.submitDataPushRequest(batch))
}
