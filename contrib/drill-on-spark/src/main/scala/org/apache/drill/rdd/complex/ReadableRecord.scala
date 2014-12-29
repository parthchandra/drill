package org.apache.drill.rdd.complex

import java.io.{ObjectOutput, ObjectInput, Externalizable}
import java.util.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.drill.exec.vector.complex.impl.NullReader
import org.apache.drill.exec.vector.complex.reader.FieldReader
import org.slf4j.LoggerFactory

import scala.language.dynamics

trait ReadableRecord extends Dynamic {
  def selectDynamic(name: String):ReadableRecord = child(name)
  def \(name: String):ReadableRecord = child(name)
  def child(name: String):ReadableRecord
  def value():Option[Any]
  def apply(i: Int):Any

  override def toString = value match {
    case Some(thing) => thing.toString
    case None => "None"
  }
}

trait Backend {
  def child(name: String): Backend
  def readObject():Any
  def readObject(index: Int):Any
}

object Backend {
  def apply(reader: FieldReader, row: Int) = {
    new FieldReaderBackend(reader, row)
  }
}

class FieldReaderBackend(reader: FieldReader, row: Int) extends Backend {
  override def child(name: String): Backend = {
    try {
      var nextReader = reader.reader(name)
      if (nextReader != NullReader.INSTANCE)
        new FieldReaderBackend(nextReader, row)
      else
        null
    } catch {
      case e: Exception => null
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

class MapReaderBackend(private var map: HashMap[String, Object]) extends Backend {
  override def child(name: String): Backend = {
    if (map == null)
      null
    else {
      try {
        // Try to change map[Key] object to another HashMap.
        // If failed it would mean map[Key] is not a map and has a scalar/literal value.
        var childMap = map.get(name).asInstanceOf[HashMap[String, Object]]
        if (childMap == null) {
          null
        } else {
          new MapReaderBackend(childMap)
        }
      } catch {
        // TODO: put the right exception type to catch.
        case e: Exception => new ScalarBackend(map.get(name))
      }
    }
  }

  override def readObject(): HashMap[String , Object] = {
    // Returns the map.
    map
  }

  // TODO
  override def readObject(index: Int): Any = {
    // Pass.
    // Return the value of the ith key of the map.
  }
}

class ScalarBackend(private var scalar: Any) extends Backend {
  // TODO
  override def child(name: String): Backend = {
    null.asInstanceOf[ScalarBackend]
  }
  override def readObject(): Any = {
    // TODO: Should return a better string representation here?
    scalar
  }
  // TODO
  override def readObject(index: Int) = {
    // If scalar is a list or tuple, return the ith item.
    // Pass
  }
}

// TODO: add a error reporting string var to return to the application.
class DrillReadableRecord() extends Externalizable with ReadableRecord {
  val logger = LoggerFactory.getLogger(getClass)
  private var recordReader = null.asInstanceOf[Backend]
  private var jsonstr : String = ""
  def this(backend: Backend) = {
    this()
    recordReader = backend
    jsonstr =  this.toString
  }
  // Should return a DrillReadableRecord here instead of readObject.
  override def apply(index: Int) = recordReader.readObject(index)

  override def child(name: String) = {
    if (recordReader != null)
      new DrillReadableRecord(recordReader.child(name))
    else
      // Can throw exception instead.
      new DrillReadableRecord(null)
  }

  override def value():Option[Any] = {
    if (recordReader == null)
      None
    else {
      recordReader.readObject match {
        case null => None
        case v: Any => Some(v)
      }
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
