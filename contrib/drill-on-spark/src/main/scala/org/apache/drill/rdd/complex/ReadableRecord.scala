package org.apache.drill.rdd.complex

import org.apache.drill.exec.vector.complex.reader.FieldReader
import org.slf4j.LoggerFactory

import scala.language.dynamics

trait ReadableRecord extends Dynamic {
  def selectDynamic(name:String):ReadableRecord = child(name)
  def \(name:String):ReadableRecord = child(name)
  def child(name:String):ReadableRecord
  def value():Option[Any]
  def apply(i:Int):Object

  override def toString = value match {
    case Some(thing) => thing.toString
    case None => "None"
  }
}

case class ReadableRecordInfo(delegate:FieldReader, row:Int, batch:Int)

class DrillReadableRecord(info:ReadableRecordInfo) extends ReadableRecord {
  val delegate = info.delegate
  val row = info.row
  val batch = info.batch
  val logger = LoggerFactory.getLogger(getClass)

  override def apply(i:Int) = {
    delegate.setPosition(row)
    delegate.readObject(i)
  }

  override def child(name:String) = new DrillReadableRecord(ReadableRecordInfo(delegate.reader(name), row, batch))

  override def value():Option[Any] = {
    delegate.setPosition(row)
    delegate.readObject match {
      case null => None
      case v:Any => Some(v)
    }
  }
}




//trait WritableRecord extends Dynamic {
//  def updateDynamic(name:String)(value:CValue) = put(name, value)
//  def put(name:String, value:CValue)
//}

//case class WritableRecord(override val fields:Map[String, CValue]) extends CObject(fields)