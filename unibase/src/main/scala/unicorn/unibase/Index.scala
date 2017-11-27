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

import unicorn.json._
import unicorn.bigtable._

/** Secondary index.
  *
  * @param indexTable The index table.
  * @param column The column or columns on which the index is based.
  *               Specify two or more column names to create a composite
  *               index on the combined values in the specified columns.
  *               List the columns to be included in the composite index,
  *               in sort-priority order.
  */
class Index(val indexTable: OrderedBigTable, column: RowKey) {

  private def indexKey(key: Array[Byte], doc: JsObject): Array[Byte] = {
    val indexKey = column(doc)
    indexKey ++ key
  }

  /** Insert a document to the index.
    *
    * @param key The row key of document in the data table.
    * @param doc the document.
    */
  def index(key: Array[Byte], doc: JsObject): Unit = {
    indexTable(indexKey(key, doc), DocumentColumnFamily, DocumentColumn) = key
  }

  /** Delete a document to the index.
    *
    * @param key The row key of document in the data table.
    * @param doc the document.
    */
  def delete(key: Array[Byte], doc: JsObject): Unit = {
    indexTable.delete(indexKey(key, doc))
  }
}

