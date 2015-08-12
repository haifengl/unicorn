package unicorn.json

import java.nio.{CharBuffer, ByteBuffer}
import java.nio.charset.Charset
import unicorn.util.Logging

/**
 * JSON Serialiizer in BSON format as defined by http://bsonspec.org/spec.html.
 * This is not fully compatible with BSON spec, where the root must be a document/JsObject.
 * In contrast, the root can be any JsValue in our implementation. Correspondingly, the
 * root will always has the type byte as the first byte.
 *
 * Not Multi-threading safe. Each thread should have its own BsonSerializer instance.
 * Data size limit to 16MB by default.
 *
 * @author Haifeng Li
 */
class BsonSerializer(buffer: ByteBuffer = ByteBuffer.allocate(16 * 1024 * 1024)) extends JsonSerializer with JsonSerializerHelper with Logging {
  def serialize(json: JsObject, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_DOCUMENT)
    if (ename.isDefined) cstring(ename.get)

    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.fields.foreach { case (field, value) => value match {
      case x: JsBoolean => serialize(x, Some(field))
      case x: JsInt     => serialize(x, Some(field))
      case x: JsLong    => serialize(x, Some(field))
      case x: JsDouble  => serialize(x, Some(field))
      case x: JsDate    => serialize(x, Some(field))
      case x: JsString  => serialize(x, Some(field))
      case x: JsBinary  => serialize(x, Some(field))
      case x: JsObject  => serialize(x, Some(field))
      case x: JsArray   => serialize(x, Some(field))
      case JsNull       => buffer.put(TYPE_NULL); cstring(field)
      case JsUndefined  => ()
    }}

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  def serialize(json: JsArray, ename: Option[String])(implicit buffer: ByteBuffer): Unit = {
    buffer.put(TYPE_ARRAY)
    if (ename.isDefined) cstring(ename.get)

    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.elements.zipWithIndex.foreach { case (value, index) => value match {
      case x: JsBoolean => serialize(x, Some(index.toString))
      case x: JsInt     => serialize(x, Some(index.toString))
      case x: JsLong    => serialize(x, Some(index.toString))
      case x: JsDouble  => serialize(x, Some(index.toString))
      case x: JsDate    => serialize(x, Some(index.toString))
      case x: JsString  => serialize(x, Some(index.toString))
      case x: JsBinary  => serialize(x, Some(index.toString))
      case x: JsObject  => serialize(x, Some(index.toString))
      case x: JsArray   => serialize(x, Some(index.toString))
      case JsNull       => buffer.put(TYPE_NULL); cstring(index.toString)
      case JsUndefined  => () // impossible
    }}

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  override def serialize(json: JsValue, jsonPath: String): Map[String, Array[Byte]] = {
    buffer.clear
    json match {
      case x: JsBoolean => serialize(x, None)(buffer)
      case x: JsInt     => serialize(x, None)(buffer)
      case x: JsLong    => serialize(x, None)(buffer)
      case x: JsDouble  => serialize(x, None)(buffer)
      case x: JsDate    => serialize(x, None)(buffer)
      case x: JsString  => serialize(x, None)(buffer)
      case x: JsBinary  => serialize(x, None)(buffer)
      case x: JsObject  => serialize(x, None)(buffer)
      case x: JsArray   => serialize(x, None)(buffer)
      case JsNull       => buffer.put(TYPE_NULL)
      case JsUndefined  => ()
    }
    Map(jsonPath -> buffer2Bytes(buffer))
  }

  override def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String): JsValue = {
    val bytes = values.get(rootJsonPath)
    if (bytes.isEmpty) throw new IllegalArgumentException(s"""root $rootJsonPath doesn't exist""")

    implicit val buffer = ByteBuffer.wrap(bytes.get)
    buffer.get match { // data type
      case TYPE_BOOLEAN   => boolean
      case TYPE_INT32     => int
      case TYPE_INT64     => long
      case TYPE_DOUBLE    => double
      case TYPE_DATETIME  => date
      case TYPE_STRING    => string
      case TYPE_BINARY    => binary
      case TYPE_NULL      => JsNull
      case TYPE_UNDEFINED => JsUndefined // should not happen
      case TYPE_DOCUMENT  => val doc = JsObject(); deserialize(doc)
      case TYPE_ARRAY     => val doc = JsObject(); deserialize(doc); doc.fields.map{case (k, v) => (k.toInt, v)}.toSeq.sortBy(_._1).map(_._2)
      case x => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
    }
  }

  def deserialize(json: JsObject)(implicit buffer: ByteBuffer): JsObject = {
    val start = buffer.position
    val size = buffer.getInt // document size

    val loop = new scala.util.control.Breaks
    loop.breakable {
      while (true) {
        buffer.get match {
          case END_OF_DOCUMENT => loop.break
          case TYPE_BOOLEAN => json(ename) = boolean
          case TYPE_INT32 => json(ename) = int
          case TYPE_INT64 => json(ename) = long
          case TYPE_DOUBLE => json(ename) = double
          case TYPE_DATETIME => json(ename) = date
          case TYPE_STRING => json(ename) = string
          case TYPE_BINARY => buffer.get; json(ename) = binary
          case TYPE_NULL => json(ename) = JsNull
          case TYPE_UNDEFINED => ename()
          case TYPE_DOCUMENT => val doc = JsObject(); json(ename) = deserialize(doc)
          case TYPE_ARRAY => val doc = JsObject(); val field = ename(); deserialize(doc); json(field) = doc.fields.map { case (k, v) => (k.toInt, v) }.toSeq.sortBy(_._1).map(_._2)
          case x => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
        }
        //println(json)
      }
    }

    if (buffer.position - start != size)
      log.warn(s"BSON size $size but deserialize finishes at ${buffer.position}, starts at $start")

    json
  }
}
