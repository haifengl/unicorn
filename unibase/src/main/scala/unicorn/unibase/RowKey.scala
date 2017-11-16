/*******************************************************************************
 * (C) Copyright 2017 Haifeng Li
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

package unicorn.unibase

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import java.sql.Timestamp

import unicorn.json._
import unicorn.oid.BsonObjectId
import unicorn.util.utf8

/** Object serialization with order preserving byte arrays.
  * Using the serialized byte arrays as row keys in HBase/Accumulo/RocksDB
  * will sort rows in the natural sort order of the object.
  *
  * Primitive (single-value) key types are: variable-length signed/unsigned
  * integers and longs, fixed-width signed/unsigned integers and longs,
  * float/double, bigdecimal, and utf-8/text/String character strings.
  *
  * Composite (multi-value) row key support is provided using struct row keys.
  * You may have an arbitrary number of fields of any type, and each field
  * may have its own sort order.
  *
  * All keys may be sorted in ascending or descending order.
  *
  * Most keys support NULL values (only fixed-width integer/long types do not).
  * All keys with NULL support treat the NULL value as comparing less than any
  * non-NULL value for sort ordering purposes.
  *
  * Some row keys, such as character strings, require an explicit termination
  * byte during serialization to indicate the end of the serialized value.
  * This terminator byte can be omitted in some situations, such as during an
  * ascending sort where the only serialized bytes come from the string row key.
  * Omitting the explicit terminator byte is known as implicit termination,
  * because the end of the serialized byte array implicitly terminates the
  * serialized value.
  *
  * If a row key is not forced to terminate, then during deserialization it
  * will read bytes up until the end of the serialized byte array. This is safe
  * if the row key serialized all of the bytes up to the end of the byte array
  * (which is the common case). However, if the user has created a custom
  * serialized format where their own extra bytes are appended to the byte array,
  * then this would produce incorrect results and explicit termination should
  * be forced.
  *
  * @author Haifeng Li
  */
sealed trait RowKey

/** A key made from only one attribute. */
sealed case class SimpleRowKey(ascending: Boolean = true, buffer: ByteBuffer = ByteBuffer.allocate(65536)) extends RowKey {

  def apply(key: JsValue): Array[Byte] = {
    key match {
      case JsInt(key) =>
      case JsLong(key) =>
      case JsDecimal(key) =>
      case JsDate(key) =>
      case JsDateTime(key) =>
      case JsTimestamp(key) =>
      case JsDouble(key) =>
      case JsString(key) =>
      case JsObjectId(key) =>
      case JsUUID(key) =>
      case _ => throw new IllegalArgumentException("Unsupported simple row key type: " + key.getClass)
    }
  }
  def apply(key: JsArray): CompoundRowKey = CompoundRowKey(key.elements.map(apply(_)).toArray)

  /** End of string */
  val END_OF_STRING               : Byte = 0x00

  /** Type markers */
  val TYPE_NULL                   : Byte = 0x00
  val TYPE_INT32                  : Byte = 0x01
  val TYPE_INT64                  : Byte = 0x02
  val TYPE_DECIMAL128             : Byte = 0x03
  val TYPE_DOUBLE                 : Byte = 0x04
  val TYPE_DATE                   : Byte = 0x05
  val TYPE_DATETIME               : Byte = 0x06
  val TYPE_TIMESTAMP              : Byte = 0x07
  val TYPE_STRING                 : Byte = 0x08
  val TYPE_ARRAY                  : Byte = 0x09
  val TYPE_OBJECTID               : Byte = 0x0A
  val TYPE_UUID                   : Byte = 0x0B

  /** Encoding of "null" */
  val `null` = Array(TYPE_NULL)

  /** Returns the byte array size to serialize a row key. */
  def sizeof(key: JsValue): Int = {
    1 + key match {
      case JsInt(_) => 4
      case JsLong(_) => 8
      case JsDecimal(_) =>
      case JsDate(_) => 8
      case JsDateTime(_) => 16
      case JsTimestamp(_) => 12
      case JsDouble(_) => 8
      case JsString(key) =>
      case JsObjectId(_) => 12
      case JsUUID(_) => 16
      case JsArray(elements) => elements.map(sizeof(_)).sum
      case _ => throw new IllegalArgumentException("Unsupported row key type: " + key.getClass)
    }
  }

  def serialize(buffer: ByteBuffer, x: Int): Unit = {
    buffer.put(TYPE_INT32)
    // We flip the leading bit so that negative values will
    // sort before 0 in ASC order for bit strings. This is
    // important as integers on JVM are all signed.
    buffer.putInt(x ^ 0x80000000)
  }

  def serialize(buffer: ByteBuffer, x: Long): Unit = {
    buffer.put(TYPE_INT64)
    // We flip the leading bit so that negative values will
    // sort before 0 in ASC order for bit strings. This is
    // important as integers on JVM are all signed.
    buffer.putLong(x ^ 0x8000000000000000L)
  }

  def serialize(buffer: ByteBuffer, x: Double): Unit = {
    buffer.put(TYPE_DOUBLE)
    buffer.putDouble(x)
  }

  def serialize(buffer: ByteBuffer, x: BigDecimal): Unit = {
    buffer.put(TYPE_DECIMAL128)
    val bytes = json.value.toPlainString.getBytes(charset)
    buffer.putInt(bytes.length)
    buffer.put(bytes)
  }

  def serialize(buffer: ByteBuffer, string: String): Unit = {
    buffer.put(string.getBytes(utf8))
    buffer.put(END_OF_STRING)
  }

  def serialize(buffer: ByteBuffer, x: LocalDate): Unit = {
    buffer.put(TYPE_DATE)
    buffer.putLong(x.toEpochDay)
  }

  def serialize(buffer: ByteBuffer, x: LocalDateTime): Unit = {
    buffer.put(TYPE_DATETIME)
    buffer.putLong(x.toLocalDate.toEpochDay)
    buffer.putLong(x.toLocalTime.toNanoOfDay)
  }

  def serialize(buffer: ByteBuffer, x: Timestamp): Unit = {
    buffer.put(TYPE_TIMESTAMP)
    buffer.putLong(x.getTime)
    buffer.putInt(x.getNanos)
  }

  def serialize(buffer: ByteBuffer, x: BsonObjectId): Unit = {
    buffer.put(TYPE_OBJECTID)
    buffer.put(x.id)
  }

  def serialize(buffer: ByteBuffer, x: UUID): Unit = {
    buffer.put(TYPE_UUID)
    buffer.putLong(x.getMostSignificantBits)
    buffer.putLong(x.getLeastSignificantBits)
  }

  def string(buffer: ByteBuffer): String = {
    val str = new collection.mutable.ArrayBuffer[Byte](64)
    var b = buffer.get
    while (b != END_OF_STRING) {str += b; b = buffer.get}
    new String(str.toArray)
  }

  def int(buffer: ByteBuffer): JsInt = {
    val x = buffer.getInt
    if (x == 0) JsInt.zero else JsInt(x)
  }

  def long(buffer: ByteBuffer): JsLong = {
    val x = buffer.getLong
    if (x == 0) JsLong.zero else JsLong(x)
  }

  def double(buffer: ByteBuffer): JsDouble = {
    val x = buffer.getDouble
    if (x == 0.0) JsDouble.zero else JsDouble(x)
  }

  def decimal(buffer: ByteBuffer): JsDecimal = {
    val length = buffer.getInt
    val dst = new Array[Byte](length)
    buffer.get(dst)
    JsDecimal(new String(dst, charset))
  }

  def date(buffer: ByteBuffer): JsDate = {
    JsDate(buffer.getLong)
  }

  def time(buffer: ByteBuffer): JsTime = {
    JsTime(buffer.getLong)
  }

  def datetime(buffer: ByteBuffer): JsDateTime = {
    val date = LocalDate.ofEpochDay(buffer.getLong)
    val time = LocalTime.ofNanoOfDay(buffer.getLong)
    JsDateTime(date, time)
  }

  def timestamp(buffer: ByteBuffer): JsTimestamp = {
    val milliseconds = buffer.getLong
    val nanos = buffer.getInt
    val timestamp = new Timestamp(milliseconds)
    timestamp.setNanos(nanos)
    JsTimestamp(timestamp)
  }

  def objectId(buffer: ByteBuffer): JsValue = {
    val id = new Array[Byte](BsonObjectId.size)
    buffer.get(id)
    JsObjectId(BsonObjectId(id))
  }
}

/** A key made from at least two attributes or simple keys,
  * only simple keys exist in a compound key.
  */
case class CompoundRowKey(key: Array[SimpleRowKey]) extends RowKey

/** Document serializer. By default, document key size is up to 64KB.
  *
  * @author Haifeng Li
  */
class RowKeySerializer(
  buffer: ByteBuffer = ByteBuffer.allocate(65536),
  val charset: Charset = utf8
) {

}