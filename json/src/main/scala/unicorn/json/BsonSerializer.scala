/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.json

import java.nio.ByteBuffer
import com.typesafe.scalalogging.Logger
import JsonSerializer._

/** JSON Serializer in BSON format as defined by http://bsonspec.org/spec.html.
  * This is not fully compatible with BSON spec, where the root must be a document/JsObject.
  * In contrast, the root can be any JsValue in our implementation. Correspondingly, the
  * root will always has the type byte as the first byte.
  *
  * Not Multi-threading safe. Each thread should have its own BsonSerializer instance.
  * Data size limit to 10MB by default.
  *
  * @author Haifeng Li
  */
class BsonSerializer(buffer: ByteBuffer = ByteBuffer.allocate(10 * 1024 * 1024)) {
  private lazy val logger = Logger(getClass)

  def serialize(json: JsValue): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def deserialize(bytes: Array[Byte]): JsValue = {
    val buffer = ByteBuffer.wrap(bytes)
    deserialize(buffer)
  }

  private def serialize(buffer: ByteBuffer, json: JsObject, ename: Option[String]): Unit = {
    buffer.put(TYPE_DOCUMENT)
    JsonSerializer.serialize(buffer, ename)

    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.fields.toSeq.sortBy(_._1).foreach { case (field, value) =>
      serialize(buffer, value, Some(field))
    }

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  private def serialize(buffer: ByteBuffer, json: JsArray, ename: Option[String]): Unit = {
    buffer.put(TYPE_ARRAY)
    JsonSerializer.serialize(buffer, ename)

    val start = buffer.position
    buffer.putInt(0) // placeholder for document size

    json.elements.zipWithIndex.foreach { case (value, index) =>
      serialize(buffer, value, Some(index.toString))
    }

    buffer.put(END_OF_DOCUMENT)
    buffer.putInt(start, buffer.position - start) // update document size
  }

  private def serialize(buffer: ByteBuffer, json: JsValue, ename: Option[String]): Unit = {
    json match {
      case x: JsBoolean  => JsonSerializer.serialize(buffer, x, ename)
      case x: JsInt      => JsonSerializer.serialize(buffer, x, ename)
      case x: JsLong     => JsonSerializer.serialize(buffer, x, ename)
      case x: JsDouble   => JsonSerializer.serialize(buffer, x, ename)
      case x: JsDecimal  => JsonSerializer.serialize(buffer, x, ename)
      case x: JsString   => JsonSerializer.serialize(buffer, x, ename)
      case x: JsDate     => JsonSerializer.serialize(buffer, x, ename)
      case x: JsTime     => JsonSerializer.serialize(buffer, x, ename)
      case x: JsDateTime => JsonSerializer.serialize(buffer, x, ename)
      case x: JsTimestamp=> JsonSerializer.serialize(buffer, x, ename)
      case x: JsUUID     => JsonSerializer.serialize(buffer, x, ename)
      case x: JsObjectId => JsonSerializer.serialize(buffer, x, ename)
      case x: JsBinary   => JsonSerializer.serialize(buffer, x, ename)
      case x: JsObject   => serialize(buffer, x, ename)
      case x: JsArray    => serialize(buffer, x, ename)
      case JsNull        => buffer.put(TYPE_NULL); JsonSerializer.serialize(buffer, ename)
      case JsUndefined   => buffer.put(TYPE_UNDEFINED); JsonSerializer.serialize(buffer, ename)
      case JsCounter(_)  => throw new IllegalArgumentException("BSON doesn't support JsCounter")
    }
  }

  private def deserialize(buffer: ByteBuffer, json: JsObject): JsObject = {
    val start = buffer.position
    val size = buffer.getInt // document size

    val loop = new scala.util.control.Breaks
    loop.breakable {
      while (true) {
        buffer.get match {
          case END_OF_DOCUMENT => loop.break
          case TYPE_BOOLEAN    => json(ename(buffer)) = boolean(buffer)
          case TYPE_INT32      => json(ename(buffer)) = int(buffer)
          case TYPE_INT64      => json(ename(buffer)) = long(buffer)
          case TYPE_DOUBLE     => json(ename(buffer)) = double(buffer)
          case TYPE_BIGDECIMAL => json(ename(buffer)) = decimal(buffer)
          case TYPE_DATE       => json(ename(buffer)) = date(buffer)
          case TYPE_TIME       => json(ename(buffer)) = time(buffer)
          case TYPE_DATETIME   => json(ename(buffer)) = datetime(buffer)
          case TYPE_TIMESTAMP  => json(ename(buffer)) = timestamp(buffer)
          case TYPE_STRING     => json(ename(buffer)) = string(buffer)
          case TYPE_OBJECTID   => json(ename(buffer)) = objectId(buffer)
          case TYPE_BINARY     => json(ename(buffer)) = binary(buffer)
          case TYPE_NULL       => json(ename(buffer)) = JsNull
          case TYPE_UNDEFINED  => json(ename(buffer)) = JsUndefined
          case TYPE_DOCUMENT   =>
            val doc = JsObject()
            json(ename(buffer)) = deserialize(buffer, doc)

          case TYPE_ARRAY      =>
            val doc = JsObject()
            val field = ename(buffer)
            deserialize(buffer, doc)
            json(field) = JsArray(doc.fields.map { case (k, v) => (k.toInt, v) }.toSeq.sortBy(_._1).map(_._2): _*)

          case x               => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
        }
      }
    }

    if (buffer.position - start != size)
      logger.warn(s"BSON size $size but deserialize finishes at ${buffer.position}, starts at $start")

    json
  }

  def deserialize(buffer: ByteBuffer): JsValue = {
    buffer.get match { // data type
      case TYPE_BOOLEAN   => boolean(buffer)
      case TYPE_INT32     => int(buffer)
      case TYPE_INT64     => long(buffer)
      case TYPE_DOUBLE    => double(buffer)
      case TYPE_BIGDECIMAL=> decimal(buffer)
      case TYPE_DATE      => date(buffer)
      case TYPE_TIME      => time(buffer)
      case TYPE_DATETIME  => datetime(buffer)
      case TYPE_TIMESTAMP => timestamp(buffer)
      case TYPE_STRING    => string(buffer)
      case TYPE_BINARY    => binary(buffer)
      case TYPE_OBJECTID  => objectId(buffer)
      case TYPE_NULL      => JsNull
      case TYPE_UNDEFINED => JsUndefined
      case TYPE_DOCUMENT  =>
        val doc = JsObject()
        deserialize(buffer, doc)

      case TYPE_ARRAY     =>
        val doc = JsObject()
        deserialize(buffer, doc)
        val elements = doc.fields.map{case (k, v) => (k.toInt, v)}.toSeq.sortBy(_._1).map(_._2)
        JsArray(elements: _*)

      case x => throw new IllegalStateException("Unsupported BSON type: %02X" format x)
    }
  }

  /** Clears the object buffer. */
  def clear: Unit = buffer.clear
}
