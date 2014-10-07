package org.apache.drill.rdd

import org.apache.spark.rdd.RDD
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

class DrillRDDSpec extends FlatSpec with Matchers with MockitoSugar {
  val registry = new RDDRegistry[DrillOutgoingRowType]
  val mockRDD = mock[RDD[DrillOutgoingRowType]]
  val rddName = "test"
  "A registry" should "register an rdd" in {
    registry.register(rddName, mockRDD)
    Seq(registry.names) should have length 1
    Seq(registry.rdds) should have length 1
    registry.find(rddName).get should be theSameInstanceAs mockRDD
  }

  it should "unregister an rdd" in {
    registry.unregister(rddName)
    registry.names should be ('empty)
    registry.rdds should be ('empty)
  }


}
