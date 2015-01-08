package org.apache.drill.rdd

/**
 * Created by rsinha on 1/8/15.
 */

import java.util.HashMap
import org.apache.drill.rdd.complex.{DrillReadableRecord, Backend, MapReaderBackend}
import org.junit.Test
import org.junit.Before

class BackendTests {
  var inputMap: HashMap[String, Object] = _
  var mapReaderBackend: MapReaderBackend = _
  var childReaderBackend: Backend = _
  var drillRecord: DrillReadableRecord = _
  var inputString: String = _
  var positiveTestRecordKeys: String = _
  var negativeTestRecordKeys: String = _
  var expectedPositiveTestOutCome: String = _
  var expectedNegativeTestOutCome: String = _

  @Before def initialize(): Unit = {
    val childOneMap: HashMap[String, HashMap[String, Integer]] = new HashMap()
    val leafMap: HashMap[String, Integer] = new HashMap()
    inputMap = new HashMap()
    leafMap.put("c", 3)
    childOneMap.put("b", leafMap)
    inputMap.put("a", childOneMap)
    mapReaderBackend = new MapReaderBackend(inputMap)
    drillRecord = new DrillReadableRecord(mapReaderBackend)
    positiveTestRecordKeys = "a.b.c"
    negativeTestRecordKeys = "a.r.z"
    expectedPositiveTestOutCome = "3"
    expectedNegativeTestOutCome = "None"
  }

  @Test def positiveTest(): Unit = {
    println("Running positive tests.")
    val testVal: String = runTest(positiveTestRecordKeys)
    assert(testVal == expectedPositiveTestOutCome, "Value returned for positive test is" + testVal +
      "whereas expected value is " + expectedPositiveTestOutCome)
    println("Successfully passed the positive tests.")

  }

  @Test def negativeTest(): Unit = {
    println("Running the negative tests.")
    val testVal: String = runTest(negativeTestRecordKeys)
    assert(testVal == expectedNegativeTestOutCome, "Value returned for negative test is"
      + testVal)
    println("Successfully passed the negative test.")
  }

  def runTest(recordKeys: String): String = {
    val recordKeysList: Array[String] = recordKeys.split("['.']")
    for (recordKey <- recordKeysList) {
      drillRecord = drillRecord.child(recordKey)
    }
    drillRecord.toString
  }
}

