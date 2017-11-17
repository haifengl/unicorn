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
import org.apache.hadoop.hbase.util.{OrderedBytes, SimplePositionedMutableByteRange}
import unicorn.json._

/** Object serialization that maintain the sort order of the original values.
  * Using the serialized byte arrays as row keys in HBase/Accumulo/RocksDB
  * will sort rows in the natural sort order of the object.
  *
  * Primitive (single-value) key types are: fixed-width signed integers and longs,
  * double, BigDecimal, Date, DateTime, Timestamp, BSON Object ID, UUID, and strings.
  * All keys may be sorted in ascending or descending order.
  *
  * Composite (multi-value) row key support is provided using struct row keys.
  * You may have an arbitrary number of fields of any primitive type, and each field
  * may have its own sort order.
  *
  * @author Haifeng Li
  */
trait RowKey {
  /** Returns the row key byte string of a JSON document. */
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
      case JsInt(key) =>
        val range = new SimplePositionedMutableByteRange(6)
        OrderedBytes.encodeNumeric(range, key, order)
        range.getBytes.slice(0, range.getPosition)
      case JsLong(key) =>
        val range = new SimplePositionedMutableByteRange(11)
        OrderedBytes.encodeNumeric(range, key, order)
        range.getBytes.slice(0, range.getPosition)
      case JsDecimal(key) =>
        val range = new SimplePositionedMutableByteRange(30)
        OrderedBytes.encodeNumeric(range, key, order)
        range.getBytes.slice(0, range.getPosition)
      case JsDouble(key) =>
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
      case JsString(key) =>
        val range = new SimplePositionedMutableByteRange(4 * key.length + 2)
        OrderedBytes.encodeString(range, key, order)
        range.getBytes.slice(0, range.getPosition)
      case JsDate(key) =>
        val buffer = ByteBuffer.allocate(10)
        buffer.put(TYPE_DATETIME)
        val range = new SimplePositionedMutableByteRange(buffer.array, 1, 9)
        OrderedBytes.encodeInt64(range, key.toEpochDay, order)
        buffer.array
      case JsDateTime(key) =>
        val buffer = ByteBuffer.allocate(19)
        buffer.put(TYPE_DATETIME)
        val range = new SimplePositionedMutableByteRange(buffer.array, 1, 18)
        OrderedBytes.encodeInt64(range, key.toLocalDate.toEpochDay, order)
        OrderedBytes.encodeInt64(range, key.toLocalTime.toNanoOfDay, order)
        buffer.array
      case JsTimestamp(key) =>
        val datetime = key.toLocalDateTime
        val buffer = ByteBuffer.allocate(19)
        buffer.put(TYPE_DATETIME)
        val range = new SimplePositionedMutableByteRange(buffer.array, 1, 18)
        OrderedBytes.encodeInt64(range, datetime.toLocalDate.toEpochDay, order)
        OrderedBytes.encodeInt64(range, datetime.toLocalTime.toNanoOfDay, order)
        buffer.array
      case JsObjectId(key) =>
        val buffer = ByteBuffer.allocate(13)
        buffer.put(TYPE_OBJECTID)
        buffer.put(key.id)
        buffer.array
      case JsUUID(key) =>
        val buffer = ByteBuffer.allocate(17)
        buffer.put(TYPE_UUID)
        buffer.putLong(key.getMostSignificantBits)
        buffer.putLong(key.getLeastSignificantBits)
        buffer.array
      case _ => throw new IllegalArgumentException("Unsupported simple row key type: " + key.getClass)
    }
  }
}

/** A key made from at least two attributes or simple keys,
  * only simple keys exist in a compound key.
  *
  * @param keys The elements of compound key.
  * @param capacity The maximum byte array size of row key.
  */
case class CompoundRowKey(keys: Seq[PrimitiveRowKey], capacity: Int = 256) extends RowKey {

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
          OrderedBytes.encodeInt64(range, key.toEpochDay, order)
        case JsDateTime(key) =>
          range.put(TYPE_DATETIME)
          OrderedBytes.encodeInt64(range, key.toLocalDate.toEpochDay, order)
          OrderedBytes.encodeInt64(range, key.toLocalTime.toNanoOfDay, order)
        case JsTimestamp(key) =>
          val datetime = key.toLocalDateTime
          range.put(TYPE_DATETIME)
          OrderedBytes.encodeInt64(range, datetime.toLocalDate.toEpochDay, order)
          OrderedBytes.encodeInt64(range, datetime.toLocalTime.toNanoOfDay, order)
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
}

object RowKey {
  def apply(key: String, order: Order = ASCENDING) = PrimitiveRowKey(key, order)

  def apply(keys: String*) = CompoundRowKey(keys.map(PrimitiveRowKey(_)))
}