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

/** Drawers are the data containers of documents. A document is simply a JSON
  * object with a unique key (primitive or compound), which is similar to the primary key
  * in relational database. The key of document serves as the row key in the underlying
  * BigTable implementation. Most BigTable implementations have the upper limit of
  * row key. Although the row key may be as long as 64KB in underlying BigTable,
  * it is always good to keep the row key (and column name) as short as possible.
  *
  * Document keys cannot be changed. The only way they can be "changed"
  * in the database is that the document is deleted and then re-inserted.
  * It pays to get the keys right the first time.
  *
  * A drawer only supports the operation to put and get the whole document by the key.
  * To get or update only some fields of document, use `Table`.
  *
  * @author Haifeng Li
  */
class Drawer(val table: BigTable, val rowkey: RowKey) {
  /** Document serializer. */
  val serializer = new JsonSerializer()

  /** The table name. */
  val name = table.name

  /** Gets a document.
    *
    * @param key document key.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Key): Option[JsValue] = {
    val cell = table(rowkey(key), DocumentColumnFamily, DocumentColumn)
    cell.map(serializer.deserialize(_))
  }

  /** Upserts a document. If a document with same key exists, it will overwritten.
    *
    * @param doc the document.
    */
  def upsert(doc: JsObject): Unit = {
    table(rowkey(doc), DocumentColumnFamily, DocumentColumn) = serializer.serialize(doc)
  }

  /** Removes a document.
    *
    * @param key the document key.
    */
  def delete(key: Key): Unit = {
    table.delete(rowkey(key))
  }
}
