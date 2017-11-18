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

package unicorn

import org.apache.hadoop.hbase.util.{Order => HBaseOrder}
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID
import unicorn.oid.BsonObjectId
import unicorn.json._

/**
  * @author Haifeng Li
  */
package object unibase {
  /** Row key sort order. */
  type Order = HBaseOrder

  /** Ascending sort order. */
  val ASCENDING  = HBaseOrder.ASCENDING
  /** Descending sort order. */
  val DESCENDING = HBaseOrder.DESCENDING

  implicit def int2Key(x: Int) = IntKey(x)
  implicit def long2Key(x: Long) = LongKey(x)
  implicit def double2Key(x: Double) = DoubleKey(x)
  implicit def bigDecimal2Key(x: BigDecimal) = BigDecimalKey(x)
  implicit def string2Key(x: String) = StringKey(x)
  implicit def date2Key(x: LocalDate) = LocalDateKey(x)
  implicit def dateTime2Key(x: LocalDateTime) = LocalDateTimeKey(x)
  implicit def timestamp2Key(x: Timestamp) = TimestampKey(x)
  implicit def uuid2Key(x: UUID) = UUIDKey(x)
  implicit def bsonObjectId2Key(x: BsonObjectId) = BsonObjectIdKey(x)

  implicit def int2Key(x: JsInt) = IntKey(x)
  implicit def long2Key(x: JsLong) = LongKey(x)
  implicit def double2Key(x: JsDouble) = DoubleKey(x)
  implicit def bigDecimal2Key(x: JsDecimal) = BigDecimalKey(x)
  implicit def string2Key(x: JsString) = StringKey(x)
  implicit def date2Key(x: JsDate) = LocalDateKey(x)
  implicit def dateTime2Key(x: JsDateTime) = LocalDateTimeKey(x)
  implicit def timestamp2Key(x: JsTimestamp) = TimestampKey(x)
  implicit def uuid2Key(x: JsUUID) = UUIDKey(x)
  implicit def bsonObjectId2Key(x: JsObjectId) = BsonObjectIdKey(x)

  implicit def json2Key(json: JsValue): Key = {
    json match {
      case JsInt(x) => IntKey(x)
      case JsLong(x) => LongKey(x)
      case JsDouble(x) => DoubleKey(x)
      case JsDecimal(x) => BigDecimalKey(x)
      case JsString(x) => StringKey(x)
      case JsDate(x) => LocalDateKey(x)
      case JsDateTime(x) => LocalDateTimeKey(x)
      case JsTimestamp(x) => TimestampKey(x)
      case JsUUID(x) => UUIDKey(x)
      case JsObjectId(x) => BsonObjectIdKey(x)
      case JsArray(elements) =>
        val keys = elements.map { e =>
          e match {
            case JsNull | JsUndefined => null
            case JsObject(_) | JsArray(_) =>
              throw new IllegalArgumentException("Compound key must consist of primitive keys: " + e)
            case _ => json2Key(e).asInstanceOf[PrimitiveKey]
          }
        }
        CompoundKey(keys)
      case _ => throw new IllegalArgumentException("Unsupported Key type: " + json)
    }
  }

  /** The first byte of the encoding indicates the content type. */
  private[unibase] val TYPE_DATETIME     : Byte = 0x50
  private[unibase] val TYPE_OBJECTID     : Byte = 0x60
  private[unibase] val TYPE_UUID         : Byte = 0x61
  private[unibase] val TYPE_ARRAY        : Byte = 0x70

  private[unibase] val NULL              = Array(0x05.toByte)
  private[unibase] val NEGATIVE_INFINITY = Array(0x07.toByte)
  private[unibase] val ZERO              = Array(0x15.toByte)
  private[unibase] val POSITIVE_INFINITY = Array(0x23.toByte)
  private[unibase] val NaN               = Array(0x25.toByte)

  private[unibase] val DefaultRowKeyField = "_id"
  private[unibase] val DefaultDocumentColumnFamily = "doc"
  private[unibase] val DefaultLocalityField = "default_locality"

  // Note that "." cannot be part of table name in Accumulo.
  private[unibase] val MetaTableName = "unicorn_meta_table"
  private[unibase] val MetaTableColumnFamily = "meta"
}
