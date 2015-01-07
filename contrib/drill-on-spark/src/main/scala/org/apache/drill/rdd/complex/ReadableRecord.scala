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
  def apply(i: Int):Any

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
  def apply(reader: FieldReader, row: Int) = {
    new FieldReaderBackend(reader, row)
  }
}

object NullReaderBackend extends Backend {
  override def child(name: String): Backend= NullReaderBackend

  override def readObject(): Any = null

  override def readObject(Index : Int) = null
}

class FieldReaderBackend(reader:FieldReader, row:Int) extends Backend {
  val logger = LoggerFactory.getLogger(getClass)
  override def child(name: String): Backend = {
    try {
      // reader.reader(name) can throw exception sometimes.
      val nextReader = reader.reader(name)
      if (nextReader != NullReader.INSTANCE && nextReader != null)
        new FieldReaderBackend(nextReader, row)
      else {
        logger.debug("Returned null reader for name", name)
        NullReaderBackend
      }
    } catch {
      case e: Exception => {
        logger.error("Caught exception while getting reader for name", name)
        NullReaderBackend
      }
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
  val logger = LoggerFactory.getLogger(getClass)
  override def child(name: String): Backend = {
    val childData = map.get(name)
    if (map != null && childData !=null) {
      childData match {
        case childMap: Map[String, Object] => {
          logger.debug("Returning child MapReaderBackend", childMap.toString)
          new MapReaderBackend(childMap)
        }
        case _ => {
          logger.debug("Returning a child GenericBackend", childData.toString)
          new GenericBackend(childData)
        }
      }
    } else {
        logger.debug("Returning a child NullReaderBackend")
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
  private var recordReader: Backend = null
  private var jsonstr: String = ""
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
    logger.debug("Serializing the string", jsonstr)
    out.writeObject(jsonstr)
  }

  def readExternal(in: ObjectInput): Unit = {
    jsonstr = in.readObject().asInstanceOf[String]
    logger.debug("After de-serializing got the string", jsonstr)
    var mapper = new ObjectMapper
    var map  = new HashMap[String, Object]()

    // Convert the jsonStr to a HashMap using mapper object.
    map = mapper.readValue(jsonstr, classOf[HashMap[String, Object]])

    // After de-serializing and converting the jsonstr to a map, set the
    // recordReader to MapReaderBackend object.
    recordReader = new MapReaderBackend(map)
  }
}
