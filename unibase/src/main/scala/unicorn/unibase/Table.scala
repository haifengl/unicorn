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

/** Similar to a `Drawer`, a `Table` is a collection of documents but can get
  * and set only some fields of documents besides the whole document. However,
  * a table may take more space on the disk.
  *
  * @author Haifeng Li
  */
class Table(val table: BigTable, val rowkey: RowKey) extends UpdateOps {
  /** Document serializer. */
  val serializer = new JsonSerializer()

  /** The table name. */
  val name = table.name

  /** Gets a document.
    *
    * @param key document key.
    * @param fields the fields to get. Get the whole document if no field is given.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Key, fields: String*): Option[JsValue] = {
    val columns = table.get(rowkey(key), DocumentColumnFamily, fields)

    if (columns.isEmpty) None
    else {
      val doc = JsObject()
      columns.foreach { case Column(qualifier, value, _) =>
        doc(qualifier) = serializer.deserialize(value)
      }
      Some(doc)
    }
  }

  /** Upserts a document. If a document with same key exists, it will overwritten.
    *
    * @param doc the document.
    * @return the document id.
    */
  def upsert(doc: JsObject): Unit = {
    table(rowkey(doc), DocumentColumnFamily, DocumentColumn) = serializer.serialize(doc)
  }

  /** Updates a document. The supported update operators include
    *
    *  - \$set: Sets the value of a field in a document.
    *  - \$unset: Removes the specified field from a document.
    *
    * @param doc the document update operators.
    */
  def update(key: Key, doc: JsObject): Unit = {
    val $set = doc("$set")
    require($set == JsUndefined || $set.isInstanceOf[JsObject], "$set is not an object: " + $set)

    val $unset = doc("$unset")
    require($unset == JsUndefined || $unset.isInstanceOf[JsObject], "$unset is not an object: " + $unset)

    if ($set.isInstanceOf[JsObject]) set(key, $set.asInstanceOf[JsObject])

    if ($unset.isInstanceOf[JsObject]) unset(key, $unset.asInstanceOf[JsObject])
  }

  /** Removes a document.
    *
    * @param key the document id.
    */
  def delete(key: Key): Unit = {
    table.delete(rowkey(key))
  }
}
