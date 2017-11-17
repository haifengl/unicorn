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

  val $id = "_id"
  val $tenant = "_tenant"

  private[unibase] val DefaultRowKeyField = "_id"

  private[unibase] val DocumentColumnFamily = "doc"

  // Note that "." cannot be part of table name in Accumulo.
  private[unibase] val MetaTableName = "unicorn_meta_table"
  private[unibase] val MetaTableColumnFamily = "meta"

  private[unibase] val DefaultLocalityField = "default_locality"
}
