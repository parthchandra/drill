package org.apache.drill.rdd

import org.apache.drill.rdd.complex._

object DrillConversions {
  implicit def toCStrIN1g(value:String) = CString(value)
  implicit def toCNumber(value:Int) = CNumber(value)
  implicit def toCNumber(value:Long) = CNumber(value)
  implicit def toCNumber(value:Double) = CNumber(value)
  implicit def toCBool(value:Boolean) = CBool(value)
  implicit def toCArray(value:Seq[CValue]) = CArray(value: _*)
}
object DrillType {
  type INTYPE = DrillIncomingRowType
  type OUTTYPE = DrillOutgoingRowType
}
