package org.apache.drill.rdd.complex

import java.io.{ObjectOutput, ObjectInput, Externalizable}

import org.apache.drill.exec.util.JsonStringHashMap
import org.apache.drill.exec.vector.complex.reader.FieldReader
import org.slf4j.LoggerFactory

import scala.language.dynamics

trait ReadableRecord extends Dynamic {
  def selectDynamic(name:String):ReadableRecord = child(name)
  def \(name:String):ReadableRecord = child(name)
  def child(name:String):ReadableRecord
  def value():Option[Any]
  def apply(i:Int):Any

  override def toString = value match {
    case Some(thing) => thing.toString
    case None => "None"
  }
}

trait Backend {
  def child(name:String): Backend
  def readObject():Any
  def readObject(index:Int):Any
}

object Backend {
  def apply(reader:FieldReader, row:Int) = {
    new FieldReaderBackend(reader, row)
  }
}

class FieldReaderBackend(reader:FieldReader, row:Int) extends Backend {

  override def child(name: String): Backend = new FieldReaderBackend(reader.reader(name), row)

  override def readObject(): Any = {
    reader.setPosition(row)
    reader.readObject()
  }

  override def readObject(index: Int): Any = {
    reader.setPosition(row)
    reader.readObject(index)
  }
}

class DrillReadableRecord(private var backend: Backend) extends Externalizable with ReadableRecord {
  val logger = LoggerFactory.getLogger(getClass)

  override def apply(index:Int) = backend.readObject(index)

  override def child(name:String) = new DrillReadableRecord(backend.child(name))

  override def value():Option[Any] = {
    backend.readObject match {
      case null => None
      case v:Any => Some(v)
    }
  }

  //TODO: implement externalizable interface
  override def writeExternal(out: ObjectOutput): Unit = {

  }

  override def readExternal(in: ObjectInput): Unit = {

  }
}
