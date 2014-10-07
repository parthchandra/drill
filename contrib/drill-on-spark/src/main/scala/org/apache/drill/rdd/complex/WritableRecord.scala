package org.apache.drill.rdd.complex


sealed abstract class CValue
case object CNothing extends CValue
case object CNull extends CValue
case class CString(value:String) extends CValue
case class CNumber(value:BigDecimal) extends CValue
case class CBool(value:Boolean) extends CValue
case class CArray(values:Vector[CValue]) extends CValue
case class CObject(fields:Map[String, CValue]) extends CValue

object CNumber {
  def apply(v:Int) = new CNumber(BigDecimal(v))
  def apply(v:Long) = new CNumber(BigDecimal(v))
  def apply(v:Double) = new CNumber(BigDecimal(v))
}

object CArray {
  def apply(values: CValue*) = new CArray(values.toVector)
}

object CObject {
  def apply(fields:(String, CValue)*) = new CObject(Map(fields:_*))
}