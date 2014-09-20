package org.apache.drill.rdd.sql

import scala.language.dynamics

import org.apache.drill.exec.vector.complex.reader.FieldReader

trait Record extends Dynamic {
  def selectDynamic(name:String):Record = child(name)
  def /(name:String):Record = child(name)
  def child(name:String):Record
  def value():Any
}

class DrillRecord(delegate:FieldReader, row:Int) extends Record {

  override def child(name:String) = new DrillRecord(delegate.reader(name), row)

  override def value():Any = {
    delegate.setPosition(row)
    delegate.readObject()
  }

}
