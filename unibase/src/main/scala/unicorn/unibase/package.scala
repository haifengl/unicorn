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

import java.util.{Date, UUID}

import unicorn.json.{JsBinary, JsDate, JsDouble, JsInt, JsLong, JsObjectId, JsString, JsUUID}
import unicorn.oid.BsonObjectId

/**
  * @author Haifeng Li
  */
package object unibase {
  val $id = "_id"
  val $tenant = "_tenant"

  private[unibase] val DocumentColumnFamily = "doc"

  // Originally we used "." as delimiter in table name.
  // However, "." cannot be part of table name in Accumulo.
  // So we switch to "_".
  private[unibase] val MetaTableName = "unicorn_meta_table"
  private[unibase] val MetaTableColumnFamily = "meta"

  private[unibase] val DefaultLocalityField = "default_locality"

  implicit def int2RowKey(x: Int) = IntRowKey(JsInt(x))
  implicit def long2RowKey(x: Long) = LongRowKey(JsLong(x))
  implicit def double2RowKey(x: Double) = DoubleRowKey(JsDouble(x))
  implicit def string2RowKey(x: String) = StringRowKey(JsString(x))
  implicit def date2RowKey(x: Date) = DateRowKey(JsDate(x))
  implicit def uuid2RowKey(x: UUID) = UuidRowKey(JsUUID(x))
  implicit def objectId2RowKey(x: BsonObjectId) = ObjectIdRowKey(JsObjectId(x))
  implicit def byteArray2JsValue(x: Array[Byte]) = BinaryRowKey(JsBinary(x))

  implicit def int2RowKey(x: JsInt) = IntRowKey(x)
  implicit def long2RowKey(x: JsLong) = LongRowKey(x)
  implicit def double2RowKey(x: JsDouble) = DoubleRowKey(x)
  implicit def string2RowKey(x: JsString) = StringRowKey(x)
  implicit def date2RowKey(x: JsDate) = DateRowKey(x)
  implicit def uuid2RowKey(x: JsUUID) = UuidRowKey(x)
  implicit def objectId2RowKey(x: JsObjectId) = ObjectIdRowKey(x)
  implicit def byteArray2JsValue(x: JsBinary) = BinaryRowKey(x)
}
