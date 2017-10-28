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

import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}
import unicorn.util._

/**
 * This JSON Serializer recursively encodes each field to a byte string, which can
 * be saved separately to a column in BigTable. The JsonPath of field can be used
 * as the column name.
 * <p>
 * Not Multi-threading safe. Each thread should have its own ColumnarJsonSerializer instance.
 * <p>
 * ByteBuffer must use BIG ENDIAN to ensure the correct byte string comparison for
 * integers and floating numbers.
 *
 * @author Haifeng Li
 */
class ColumnarJsonSerializer(
  buffer: ByteBuffer = ByteBuffer.allocate(16 * 1024 * 1024),
  val charset: Charset = utf8,
  val root: String = "$",
  val pathDelimiter: String = "."
) extends BaseJsonSerializer {

  require(buffer.order == ByteOrder.BIG_ENDIAN)

  private def jsonPath(parent: String, field: String) = s"%s${pathDelimiter}%s".format(parent, field)

  private def jsonPath(parent: String, index: Int) = s"%s${pathDelimiter}%d".format(parent, index)

  /** Serialize a string to C null-terminated string. */
  def serialize(string: String): Array[Byte] = {
    buffer.clear
    val bytes = string.getBytes(charset)
    buffer.put(bytes)
    buffer.put(END_OF_STRING)
    buffer
  }

  def serialize(json: JsBoolean): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsInt): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsLong): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsDouble): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsString): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsDate): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsTime): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsDateTime): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsTimestamp): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsObjectId): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsUUID): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsBinary): Array[Byte] = {
    buffer.clear
    serialize(buffer, json, None)
    buffer
  }

  def serialize(json: JsCounter): Array[Byte] = {
    buffer.clear
    buffer.putLong(json.value)
    buffer
  }

  private def serialize(json: JsObject, root: String, map: collection.mutable.Map[String, Array[Byte]]): Unit = {
    buffer.clear
    buffer.put(TYPE_DOCUMENT)
    json.fields.foreach { case (field, _) => serialize(buffer, Some(field)) }
    map(root) = buffer

    json.fields.foreach { case (field, value) =>
      serialize(value, jsonPath(root, field), map)
    }
  }

  private def serialize(json: JsArray, root: String, map: collection.mutable.Map[String, Array[Byte]]): Unit = {
    buffer.clear
    buffer.put(TYPE_ARRAY)
    buffer.putInt(json.elements.size)
    map(root) = buffer

    json.elements.zipWithIndex.foreach { case (value, index) =>
      serialize(value, jsonPath(root, index), map)
    }
  }

  private def serialize(json: JsValue, jsonPath: String, map: collection.mutable.Map[String, Array[Byte]]): Unit = {
    json match {
      case x: JsBoolean => map(jsonPath) = serialize(x)
      case x: JsInt => map(jsonPath) = serialize(x)
      case x: JsLong => map(jsonPath) = serialize(x)
      case x: JsCounter => map(jsonPath) = serialize(x)
      case x: JsDouble => map(jsonPath) = serialize(x)
      case x: JsString => map(jsonPath) = serialize(x)
      case x: JsDate => map(jsonPath) = serialize(x)
      case x: JsTime => map(jsonPath) = serialize(x)
      case x: JsDateTime => map(jsonPath) = serialize(x)
      case x: JsTimestamp => map(jsonPath) = serialize(x)
      case x: JsUUID => map(jsonPath) = serialize(x)
      case x: JsObjectId => map(jsonPath) = serialize(x)
      case x: JsBinary => map(jsonPath) = serialize(x)
      case JsNull => map(jsonPath) = `null`
      case JsUndefined => map(jsonPath) = undefined
      case x: JsObject => serialize(x, jsonPath, map)
      case x: JsArray => serialize(x, jsonPath, map)
    }
  }

  override def serialize(json: JsValue, jsonPath: String): Map[String, Array[Byte]] = {
    val map = collection.mutable.Map[String, Array[Byte]]()
    serialize(json, jsonPath, map)
    map.toMap
  }

  override def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String): JsValue = {
    val bytes = values.get(rootJsonPath)
    if (bytes.isEmpty) return JsUndefined

    implicit val buffer = ByteBuffer.wrap(bytes.get)
    buffer.get match {
      // data type
      case TYPE_BOOLEAN   => boolean(buffer)
      case TYPE_INT32     => int(buffer)
      case TYPE_INT64     => long(buffer)
      case END_OF_DOCUMENT | TYPE_MINKEY if bytes.get.length == 8 => JsCounter(buffer.getLong(0)) // hacking counter
      case TYPE_DOUBLE    => double(buffer)
      case TYPE_DATE      => date(buffer)
      case TYPE_TIME      => time(buffer)
      case TYPE_DATETIME  => datetime(buffer)
      case TYPE_TIMESTAMP => timestamp(buffer)
      case TYPE_STRING    => string(buffer)
      case TYPE_OBJECTID  => objectId(buffer)
      case TYPE_BINARY    => binary(buffer)
      case TYPE_NULL      => JsNull
      case TYPE_UNDEFINED => JsUndefined
      case TYPE_DOCUMENT  =>
        val keys = fields(buffer)
        val kv = keys.map { key => (key, deserialize(values, jsonPath(rootJsonPath, key))) }.filter(_._2 != JsUndefined)
        JsObject(kv: _*)

      case TYPE_ARRAY     =>
        val size = buffer.getInt
        val elements = 0.until(size) map { index => deserialize(values, jsonPath(rootJsonPath, index)) }
        JsArray(elements: _*)

      case x              => throw new IllegalStateException("Unsupported JSON type: %02X" format x)
    }
  }

  private def fields(buffer: ByteBuffer): Seq[String] = {
    val keys = collection.mutable.ArrayBuffer[String]()
    while (buffer.hasRemaining) {
      keys += cstring(buffer)
    }
    keys
  }
}
