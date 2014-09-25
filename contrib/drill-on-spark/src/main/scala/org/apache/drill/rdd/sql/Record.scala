package org.apache.drill.rdd.sql

import org.slf4j.LoggerFactory

import scala.language.dynamics

import org.apache.drill.exec.vector.complex.reader.FieldReader

trait Record extends Dynamic {
  def selectDynamic(name:String):Record = child(name)
  def /(name:String):Record = child(name)
  def child(name:String):Record
  def value():Option[Any]
  def apply(i:Int):Object

  override def toString = value match {
    case Some(thing) => thing.toString
    case None => "None"
  }
}

case class RecordInfo(delegate:FieldReader, row:Int, batch:Int)

class DrillRecord(info:RecordInfo) extends Record {
  val delegate = info.delegate
  val row = info.row
  val batch = info.batch
  val logger = LoggerFactory.getLogger(getClass)

  override def apply(i:Int) = {
    delegate.setPosition(row)
    delegate.readObject(i)
  }

  override def child(name:String) = new DrillRecord(RecordInfo(delegate.reader(name), row, batch))

  override def value():Option[Any] = {
    delegate.setPosition(row)
    delegate.readObject match {
      case null => None
      case v:Any => Some(v)
    }
  }

}
