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

import unicorn.json.JsObject
import unicorn.bigtable.OrderedBigTable
import unicorn.kv.OrderedKeyspace

/** Secondary index.
  *
  */
trait Index {
  /** The column or columns on which the index is based.
    * Specify two or more column names to create a composite
    * index on the combined values in the specified columns.
    * List the columns to be included in the composite index,
    * in sort-priority order.
    */
  val column: RowKey

  private[unibase] def indexKey(key: Array[Byte], doc: JsObject): Array[Byte] = {
    val indexKey = column.serialize(doc)
    indexKey ++ key
  }

  /** Insert a document to the index.
    *
    * @param key The row key of document in the data table.
    * @param doc the document.
    */
  def put(key: Array[Byte], doc: JsObject): Unit

  /** Delete a document to the index.
    *
    * @param key The row key of document in the data table.
    * @param doc the document.
    */
  def delete(key: Array[Byte], doc: JsObject): Unit
}

/** BigTable based secondary index.
  *
  * @param indexTable The index table.
  * @param column The column or columns on which the index is based.
  *               Specify two or more column names to create a composite
  *               index on the combined values in the specified columns.
  *               List the columns to be included in the composite index,
  *               in sort-priority order.
  */
class KeyValueIndex(val indexTable: OrderedKeyspace, override val column: RowKey) extends Index {

  override def put(key: Array[Byte], doc: JsObject): Unit = {
    indexTable(indexKey(key, doc)) = key
  }

  override def delete(key: Array[Byte], doc: JsObject): Unit = {
    indexTable.delete(indexKey(key, doc))
  }
}

/** BigTable based secondary index.
  *
  * @param indexTable The index table.
  * @param column The column or columns on which the index is based.
  *               Specify two or more column names to create a composite
  *               index on the combined values in the specified columns.
  *               List the columns to be included in the composite index,
  *               in sort-priority order.
  */
class BigTableIndex(val indexTable: OrderedBigTable, override val column: RowKey) extends Index {

  override def put(key: Array[Byte], doc: JsObject): Unit = {
    indexTable(indexKey(key, doc), DocumentColumnFamily, DocumentColumn) = key
  }

  override def delete(key: Array[Byte], doc: JsObject): Unit = {
    indexTable.delete(indexKey(key, doc))
  }
}
