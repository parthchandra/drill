package org.apache.drill.rdd.query

import org.apache.drill.exec.client.DrillClient
import org.apache.drill.exec.proto.CoordinationProtos
import org.apache.drill.exec.proto.UserProtos.{QueryFragmentQuery, QueryPlanFragments}
import org.apache.drill.exec.rpc.user.UserResultsListener

import scala.util.Try


class ExtendedDrillClient {
  private val client = new DrillClient

  def connect(endpoint:Option[CoordinationProtos.DrillbitEndpoint]=None):Try[ExtendedDrillClient] = {
    Try(
      endpoint match {
        case Some(addr) => client.connect(addr); this
        case None => client.connect(); this
      }
    )
  }

  def close() = Try(client.close())

  def getPlanFor(sql:String):Try[QueryPlanFragments] = Try(client.planQuery(sql).get)

  def getFragment(query:QueryFragmentQuery, listener:UserResultsListener) =
    Try(client.submitReadFragmentRequest(query, listener))
}
