package org.apache.drill.rdd.complex

import java.io.{ObjectOutput, ObjectInput, Externalizable}
import java.util.{HashMap, Map}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.drill.exec.vector.complex.impl.NullReader
import org.apache.drill.exec.vector.complex.reader.FieldReader
import org.slf4j.LoggerFactory
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import scala.reflect.runtime.{universe => ru}

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

object ObjectType {
  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
}

object NullReaderBackend extends Backend {
  override def child(name: String): Backend= NullReaderBackend

  override def readObject(): Any = null

  override def readObject(Index : Int) = null
}

class FieldReaderBackend(reader:FieldReader, row:Int) extends Backend {
  override def child(name: String): Backend = {
    try {
      // reader.reader(name) can throw exception sometimes.
      var nextReader = reader.reader(name)
      if (nextReader != NullReader.INSTANCE && nextReader != null)
        new FieldReaderBackend(nextReader, row)
      else
        NullReaderBackend
    } catch {
      case e: Exception => NullReaderBackend
    }
  }

  override def readObject(): Any = {
    reader.setPosition(row)
    reader.readObject()
  }

  override def readObject(index: Int): Any = {
    reader.setPosition(row)
    reader.readObject(index)
  }
}

class MapReaderBackend(map: Map[String, Object]) extends Backend {
  override def child(name: String): Backend = {
    var childMap = map.get(name)
    if (map != null && childMap !=null) {
        var objType = ObjectType.getTypeTag(childMap)
        if (objType.tpe.toString.indexOf("HashMap") == -1)
          new GenericBackend(childMap)
        else {
          new MapReaderBackend(childMap.asInstanceOf[HashMap[String, Object]])
        }
    } else {
        NullReaderBackend
    }
  }



  override def readObject(): Map[String , Object] = {
    // Returns the map.
    map
  }

  // Not supported by the MapReaderBackend.
  override def readObject(index: Int): Any = {
    throw new UnsupportedOperationException("readObject(index: Int) is not" +
      " supported by MapReaderBackend.")
  }
}

class GenericBackend(genericData: Any) extends Backend {
  override def child(name: String): Backend = {
    NullReaderBackend
  }

  override def readObject(): Any = {
    genericData
  }
  // TODO: to provide proper implementation for list.
  override def readObject(index: Int): Any = {
    throw new NotImplementedException
  }
}

class DrillReadableRecord() extends Externalizable with ReadableRecord {
  val logger = LoggerFactory.getLogger(getClass)
  private var recordReader = null.asInstanceOf[Backend]
  private var jsonstr : String = ""
  def this(backend: Backend) = {
    this()
    recordReader = backend
    jsonstr =  this.toString
  }

  override def apply(index: Int) = recordReader.readObject(index)

  override def child(name: String) = new DrillReadableRecord(recordReader.child(name))

  override def value():Option[Any] = {
    recordReader.readObject match {
      case null => None
      case v: Any => Some(v)
    }
  }

  def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(jsonstr)
  }

  def readExternal(in: ObjectInput): Unit = {
    jsonstr = in.readObject().asInstanceOf[String]
    var mapper = new ObjectMapper
    var map  = new HashMap[String, Object]()

    // Convert the jsonStr to a HashMap using mapper object.
    map = mapper.readValue(jsonstr, classOf[HashMap[String, Object]])

    // After de-serializing and converting the jsonstr to a map, set the
    // recordReader to MapReaderBackend object.
    recordReader = new MapReaderBackend(map)
  }
}
