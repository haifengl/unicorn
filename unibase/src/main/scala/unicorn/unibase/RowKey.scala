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
  * Keep the row keys as short as is reasonable such that they can still be
  * useful for required data access. Long row key and column names have
  * high overhead in many BigTable implementations.
  *
  * @author Haifeng Li
  */
trait RowKey {
  /** Serializes a document key. */
  def apply(key: Key): Array[Byte]

  /** Extracts and serializes the document key of a JSON document. */
  def apply(json: JsObject): Array[Byte]
}

/** A key with only one primitive attribute.
  *
  * @param key the field path of row key in the JsObject.
  * @param order sort order.
  */
case class PrimitiveRowKey(key: String, order: Order = ASCENDING) extends RowKey {
  override def apply(json: JsObject): Array[Byte] = {
    json(key) match {
      case JsInt(key) => RowKey(key, order)
      case JsLong(key) => RowKey(key, order)
      case JsDecimal(key) => RowKey(key, order)
      case JsDouble(key) => RowKey(key, order)
      case JsString(key) => RowKey(key, order)
      case JsDate(key) => RowKey(key, order)
      case JsDateTime(key) => RowKey(key, order)
      case JsTimestamp(key) => RowKey(key, order)
      case JsObjectId(key) => RowKey(key, order)
      case JsUUID(key) => RowKey(key, order)
      case _ => throw new IllegalArgumentException("Unsupported primitive row key type: " + key.getClass)
    }
  }

  override def apply(key: Key): Array[Byte] = {
    key match {
      case IntKey(key) => RowKey(key, order)
      case LongKey(key) => RowKey(key, order)
      case DecimalKey(key) => RowKey(key, order)
      case DoubleKey(key) => RowKey(key, order)
      case StringKey(key) => RowKey(key, order)
      case DateKey(key) => RowKey(key, order)
      case DateTimeKey(key) => RowKey(key, order)
      case TimestampKey(key) => RowKey(key, order)
      case ObjectIdKey(key) => RowKey(key, order)
      case UUIDKey(key) => RowKey(key, order)
      case _ => throw new IllegalArgumentException("Unsupported primitive row key type: " + key.getClass)
    }
  }
}

/** A composite key is made from multiple primitive keys,
  *
  * @param keys The elements of compound key.
  * @param capacity The maximum byte array size of row key.
  *                 The default value 256 should be large enough
  *                 for most applications. If longer keys are
  *                 needed, think again.
  */
case class CompositeRowKey(keys: Seq[PrimitiveRowKey], capacity: Int = 256) extends RowKey {

  override def apply(json: JsObject): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(capacity)
    keys.foreach { case PrimitiveRowKey(key, order) =>
      json(key) match {
        case JsInt(key) => OrderedBytes.encodeNumeric(range, key, order)
        case JsLong(key) => OrderedBytes.encodeNumeric(range, key, order)
        case JsDecimal(key) => OrderedBytes.encodeNumeric(range, key, order)
        case JsDouble(key) =>
          key match {
            case 0.0 => range.put(ZERO(0))
            case Double.NaN => range.put(NaN(0))
            case Double.NegativeInfinity => range.put(NEGATIVE_INFINITY(0))
            case Double.PositiveInfinity => range.put(POSITIVE_INFINITY(0))
            case _ => OrderedBytes.encodeNumeric(range, key, order)
          }
        case JsString(key) => OrderedBytes.encodeString(range, key, order)
        case JsDate(key) =>
          range.put(TYPE_DATETIME)
          val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
          OrderedBytes.encodeInt32(range, date, order)
        case JsDateTime(key) =>
          range.put(TYPE_DATETIME)
          val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
          val time = key.getHour * 10000 + key.getMinute * 100 + key.getSecond
          OrderedBytes.encodeInt32(range, date, order)
          OrderedBytes.encodeInt32(range, time, order)
        case JsTimestamp(key) =>
          range.put(TYPE_DATETIME)
          val datetime = key.toLocalDateTime
          val date = datetime.getYear * 10000 + datetime.getMonthValue * 100 + datetime.getDayOfMonth
          val time = datetime.getHour * 10000 + datetime.getMinute * 100 + datetime.getSecond
          OrderedBytes.encodeInt32(range, date, order)
          OrderedBytes.encodeInt32(range, time, order)
          OrderedBytes.encodeInt32(range, key.getNanos, order)
        case JsObjectId(key) =>
          range.put(TYPE_OBJECTID)
          range.put(key.id)
        case JsUUID(key) =>
          range.put(TYPE_UUID)
          range.putLong(key.getMostSignificantBits)
          range.putLong(key.getLeastSignificantBits)
        case JsNull | JsUndefined =>
          OrderedBytes.encodeNull(range, order)
        case _ => throw new IllegalArgumentException("Unsupported simple row key type: " + key.getClass)
      }
    }

    range.getBytes.slice(0, range.getPosition)
  }

  override def apply(key: Key): Array[Byte] = {
    key match {
      case IntKey(key) => RowKey(key, keys(0).order)
      case LongKey(key) => RowKey(key, keys(0).order)
      case DecimalKey(key) => RowKey(key, keys(0).order)
      case DoubleKey(key) => RowKey(key, keys(0).order)
      case StringKey(key) => RowKey(key, keys(0).order)
      case DateKey(key) => RowKey(key, keys(0).order)
      case DateTimeKey(key) => RowKey(key, keys(0).order)
      case TimestampKey(key) => RowKey(key, keys(0).order)
      case ObjectIdKey(key) => RowKey(key, keys(0).order)
      case UUIDKey(key) => RowKey(key, keys(0).order)
      case CompositeKey(keys) =>
        if (keys.size > this.keys.size)
          throw new IllegalArgumentException("Too many composite key elements")

        val range = new SimplePositionedMutableByteRange(capacity)
        keys.zip(this.keys.slice(0, keys.size).map(_.order)).foreach {
          case (IntKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (LongKey(key), order) => OrderedBytes.encodeNumeric(range, key, order)
          case (DecimalKey(key), order) => OrderedBytes.encodeNumeric(range, key , order)
          case (DoubleKey(key), order) =>
            key match {
              case 0.0 => range.put(ZERO(0))
              case Double.NaN => range.put(NaN(0))
              case Double.NegativeInfinity => range.put(NEGATIVE_INFINITY(0))
              case Double.PositiveInfinity => range.put(POSITIVE_INFINITY(0))
              case _ => OrderedBytes.encodeNumeric(range, key, order)
            }
          case (StringKey(key), order) => OrderedBytes.encodeString(range, key, order)
          case (DateKey(key), order) =>
            range.put(TYPE_DATETIME)
            val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
            OrderedBytes.encodeInt32(range, date, order)
          case (DateTimeKey(key), order) =>
            range.put(TYPE_DATETIME)
            val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
            val time = key.getHour * 10000 + key.getMinute * 100 + key.getSecond
            OrderedBytes.encodeInt32(range, date, order)
            OrderedBytes.encodeInt32(range, time, order)
          case (TimestampKey(key), order) =>
            range.put(TYPE_DATETIME)
            val datetime = key.toLocalDateTime
            val date = datetime.getYear * 10000 + datetime.getMonthValue * 100 + datetime.getDayOfMonth
            val time = datetime.getHour * 10000 + datetime.getMinute * 100 + datetime.getSecond
            OrderedBytes.encodeInt32(range, date, order)
            OrderedBytes.encodeInt32(range, time, order)
            OrderedBytes.encodeInt32(range, key.getNanos, order)
          case (ObjectIdKey(key), order) =>
            range.put(TYPE_OBJECTID)
            range.put(key.id)
          case (UUIDKey(key), order) =>
            range.put(TYPE_UUID)
            range.putLong(key.getMostSignificantBits)
            range.putLong(key.getLeastSignificantBits)
          case (null, order) =>
            OrderedBytes.encodeNull(range, order)
        }
        range.getBytes.slice(0, range.getPosition)
    }
  }
}

object RowKey {
  def apply(key: String) = PrimitiveRowKey(key)

  def apply(keys: String*) = CompositeRowKey(keys.map(PrimitiveRowKey(_)))

  def apply(key: Key, capacity: Int = 256): Array[Byte] = {
    key match {
      case IntKey(key) => apply(key, ASCENDING)
      case LongKey(key) => apply(key, ASCENDING)
      case DecimalKey(key) => apply(key, ASCENDING)
      case DoubleKey(key) => apply(key, ASCENDING)
      case StringKey(key) => apply(key, ASCENDING)
      case DateKey(key) => apply(key, ASCENDING)
      case DateTimeKey(key) => apply(key, ASCENDING)
      case TimestampKey(key) => apply(key, ASCENDING)
      case ObjectIdKey(key) => apply(key, ASCENDING)
      case UUIDKey(key) => apply(key, ASCENDING)
      case CompositeKey(keys) =>
        val range = new SimplePositionedMutableByteRange(capacity)
        keys.foreach {
          case IntKey(key) => OrderedBytes.encodeNumeric(range, key, ASCENDING)
          case LongKey(key) => OrderedBytes.encodeNumeric(range, key, ASCENDING)
          case DecimalKey(key) => OrderedBytes.encodeNumeric(range, key , ASCENDING)
          case DoubleKey(key) =>
            key match {
              case 0.0 => range.put(ZERO(0))
              case Double.NaN => range.put(NaN(0))
              case Double.NegativeInfinity => range.put(NEGATIVE_INFINITY(0))
              case Double.PositiveInfinity => range.put(POSITIVE_INFINITY(0))
              case _ => OrderedBytes.encodeNumeric(range, key, ASCENDING)
            }
          case StringKey(key) => OrderedBytes.encodeString(range, key, ASCENDING)
          case DateKey(key) =>
            range.put(TYPE_DATETIME)
            val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
            OrderedBytes.encodeInt32(range, date, ASCENDING)
          case DateTimeKey(key) =>
            range.put(TYPE_DATETIME)
            val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
            val time = key.getHour * 10000 + key.getMinute * 100 + key.getSecond
            OrderedBytes.encodeInt32(range, date, ASCENDING)
            OrderedBytes.encodeInt32(range, time, ASCENDING)
          case TimestampKey(key) =>
            range.put(TYPE_DATETIME)
            val datetime = key.toLocalDateTime
            val date = datetime.getYear * 10000 + datetime.getMonthValue * 100 + datetime.getDayOfMonth
            val time = datetime.getHour * 10000 + datetime.getMinute * 100 + datetime.getSecond
            OrderedBytes.encodeInt32(range, date, ASCENDING)
            OrderedBytes.encodeInt32(range, time, ASCENDING)
            OrderedBytes.encodeInt32(range, key.getNanos, ASCENDING)
          case ObjectIdKey(key) =>
            range.put(TYPE_OBJECTID)
            range.put(key.id)
          case UUIDKey(key) =>
            range.put(TYPE_UUID)
            range.putLong(key.getMostSignificantBits)
            range.putLong(key.getLeastSignificantBits)
          case null =>
            OrderedBytes.encodeNull(range, ASCENDING)
        }
        range.getBytes.slice(0, range.getPosition)
    }
  }

  def apply(key: Int, order: Order): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(6)
    OrderedBytes.encodeNumeric(range, key, order)
    range.getBytes.slice(0, range.getPosition)
  }

  def apply(key: Long, order: Order): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(11)
    OrderedBytes.encodeNumeric(range, key, order)
    range.getBytes.slice(0, range.getPosition)
  }

  def apply(key: Double, order: Order): Array[Byte] = {
    key match {
      case 0.0 => ZERO
      case Double.NaN => NaN
      case Double.NegativeInfinity => NEGATIVE_INFINITY
      case Double.PositiveInfinity => POSITIVE_INFINITY
      case _ =>
        val range = new SimplePositionedMutableByteRange(11)
        OrderedBytes.encodeNumeric(range, key, order)
        range.getBytes.slice(0, range.getPosition)
    }
  }

  def apply(key: BigDecimal, order: Order): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(30)
    OrderedBytes.encodeNumeric(range, key, order)
    range.getBytes.slice(0, range.getPosition)
  }

  def apply(key: String, order: Order): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(4 * key.length + 2)
    OrderedBytes.encodeString(range, key, order)
    range.getBytes.slice(0, range.getPosition)
  }

  def apply(key: LocalDate, order: Order): Array[Byte] = {
    val buffer = ByteBuffer.allocate(6)
    buffer.put(TYPE_DATETIME)
    val range = new SimplePositionedMutableByteRange(buffer.array, 1, 5)
    val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
    OrderedBytes.encodeInt32(range, date, order)
    buffer.array
  }

  def apply(key: LocalDateTime, order: Order): Array[Byte] = {
    val buffer = ByteBuffer.allocate(11)
    buffer.put(TYPE_DATETIME)
    val range = new SimplePositionedMutableByteRange(buffer.array, 1, 10)
    val date = key.getYear * 10000 + key.getMonthValue * 100 + key.getDayOfMonth
    val time = key.getHour * 10000 + key.getMinute * 100 + key.getSecond
    OrderedBytes.encodeInt32(range, date, order)
    OrderedBytes.encodeInt32(range, time, order)
    buffer.array
  }

  def apply(key: Timestamp, order: Order): Array[Byte] = {
    val datetime = key.toLocalDateTime
    val buffer = ByteBuffer.allocate(16)
    buffer.put(TYPE_DATETIME)
    val range = new SimplePositionedMutableByteRange(buffer.array, 1, 15)
    val date = datetime.getYear * 10000 + datetime.getMonthValue * 100 + datetime.getDayOfMonth
    val time = datetime.getHour * 10000 + datetime.getMinute * 100 + datetime.getSecond
    OrderedBytes.encodeInt32(range, date, order)
    OrderedBytes.encodeInt32(range, time, order)
    OrderedBytes.encodeInt32(range, key.getNanos, order)
    buffer.array
  }

  def apply(key: UUID, order: Order): Array[Byte] = {
    val buffer = ByteBuffer.allocate(17)
    buffer.put(TYPE_UUID)
    buffer.putLong(key.getMostSignificantBits)
    buffer.putLong(key.getLeastSignificantBits)
    buffer.array
  }

  def apply(key: ObjectId, order: Order): Array[Byte] = {
    val buffer = ByteBuffer.allocate(13)
    buffer.put(TYPE_OBJECTID)
    buffer.put(key.id)
    buffer.array
  }
}