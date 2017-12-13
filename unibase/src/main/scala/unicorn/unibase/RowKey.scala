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

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.util.{OrderedBytes, SimplePositionedMutableByteRange}
import unicorn.json._

/** Serializes document keys in the way that maintain the sort order of the original values.
  * Using the serialized byte arrays as row keys in HBase/Accumulo/RocksDB
  * will sort rows in the natural sort order of the object.
  *
  * Primitive key types are: Int, Long, Double, BigDecimal,
  * LocalDate, LocalDateTime, Timestamp, BSON Object ID, UUID, and Strings.
  * Composite key may have an arbitrary number of fields of any primitive type.
  *
  * It is important to keep the row keys as short as reasonable.
  * Long row key and column names have high overhead in many BigTable implementations.
  *
  * @param fields The elements of key.
  * @param capacity The maximum byte array size of row key.
  *                 The default value 256 should be large enough
  *                 for most applications. If longer keys are
  *                 needed, think again.
  *
  * @author Haifeng Li
  */
case class RowKey(fields: Seq[(String, Order)], capacity: Int = 256) {

  require(fields.size > 0)
  val range = new SimplePositionedMutableByteRange(capacity)

  def serialize(json: JsObject): Array[Byte] = {
    if (fields.size == 1)
      serialize(json(fields(0)._1))
    else
      serialize(CompositeKey(fields.map { case (key, _) => json2Key(json(key)).asInstanceOf[PrimitiveKey] }))
  }

  def serialize(key: Key): Array[Byte] = {
    range.setPosition(0)
    key match {
      case IntKey(key) => OrderedBytes.encodeNumeric(range, key, fields(0)._2)
      case LongKey(key) => OrderedBytes.encodeNumeric(range, key, fields(0)._2)
      case DecimalKey(key) => OrderedBytes.encodeNumeric(range, key, fields(0)._2)
      case DoubleKey(key) => OrderedBytes.encodeNumeric(range, key, fields(0)._2)
      case StringKey(key) => OrderedBytes.encodeString(range, key, fields(0)._2)
      case DateKey(key) => serialize(range, key, fields(0)._2)
      case DateTimeKey(key) => serialize(range, key, fields(0)._2)
      case TimestampKey(key) => serialize(range, key, fields(0)._2)
      case ObjectIdKey(key) => serialize(range, key, fields(0)._2)
      case UUIDKey(key) => serialize(range, key, fields(0)._2)
      case CompositeKey(keys) =>
        keys.zip(fields.slice(0, keys.size).map(_._2)).foreach {
          case (IntKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (LongKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (DecimalKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (DoubleKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (StringKey(key), order) => OrderedBytes.encodeString(range, key, order)
          case (DateKey(key), order) => serialize(range, key, order)
          case (DateTimeKey(key), order) => serialize(range, key, order)
          case (TimestampKey(key), order) => serialize(range, key, order)
          case (ObjectIdKey(key), order) => serialize(range, key, order)
          case (UUIDKey(key), order) => serialize(range, key, order)
          case (null, order) => OrderedBytes.encodeNull(range, order)
        }
    }
    range.getBytes.slice(0, range.getPosition)
  }

  def serialize(range: SimplePositionedMutableByteRange, key: LocalDate, order: Order): Unit = {
    range.put(TYPE_DATETIME)
    val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
    OrderedBytes.encodeInt32(range, date, order)
  }

  def serialize(range: SimplePositionedMutableByteRange, key: LocalDateTime, order: Order): Unit = {
    range.put(TYPE_DATETIME)
    val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
    val time = key.getHour * 10000 + key.getMinute * 100 + key.getSecond
    OrderedBytes.encodeInt32(range, date, order)
    OrderedBytes.encodeInt32(range, time, order)
  }

  def serialize(range: SimplePositionedMutableByteRange, key: Timestamp, order: Order): Unit = {
    range.put(TYPE_DATETIME)
    val datetime = key.toLocalDateTime
    val date = datetime.getYear * 10000 + datetime.getMonthValue * 100 + datetime.getDayOfMonth
    val time = datetime.getHour * 10000 + datetime.getMinute * 100 + datetime.getSecond
    OrderedBytes.encodeInt32(range, date, order)
    OrderedBytes.encodeInt32(range, time, order)
    OrderedBytes.encodeInt32(range, key.getNanos, order)
  }

  def serialize(range: SimplePositionedMutableByteRange, key: UUID, order: Order): Unit = {
    range.put(TYPE_UUID)
    range.putLong(key.getMostSignificantBits)
    range.putLong(key.getLeastSignificantBits)
  }

  def serialize(range: SimplePositionedMutableByteRange, key: ObjectId, order: Order): Unit = {
    range.put(TYPE_OBJECTID)
    range.put(key.id)
  }
}

object RowKey {
  def apply(keys: String*) = new RowKey(keys.map((_, ASCENDING)))
}