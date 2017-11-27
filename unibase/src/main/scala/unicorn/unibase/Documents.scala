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

import unicorn.bigtable.BigTable
import unicorn.json.{JsObject, JsonSerializer}
import unicorn.kv.Keyspace

/** A collection of documents. A document is simply a JSON object with
  * a unique key (primitive or composite), which is similar to the primary
  * key in relational database.
  *
  * Document keys cannot be changed. The only way they can be "changed"
  * in the database is that the document is deleted and then re-inserted.
  * It pays to get the keys right the first time.
  *
  * @author Haifeng Li
  */
trait Documents {
  /** The name of document collection. */
  val name: String

  /** Gets a document by row key.
    *
    * @param key row key.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Array[Byte]): Option[JsObject]

  /** Gets a document.
    *
    * @param key document key.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Key): Option[JsObject]

  /** Upserts a document. If a document with same key exists, it will overwritten.
    *
    * @param doc the document.
    */
  def upsert(doc: JsObject): Unit

  /** Removes a document.
    *
    * @param key the document key.
    */
  def delete(key: Key): Unit
}


/** Document collection by key-value store. The key of document serves
  * as the key in the underlying key-value store.
  *
  * @author Haifeng Li
  */
class KeyValueDocuments(val table: Keyspace, val rowkey: RowKey) extends Documents {
  /** Document serializer. */
  val serializer = new JsonSerializer()

  /** The table name. */
  override val name = table.name

  override def apply(key: Array[Byte]): Option[JsObject] = {
    val cell = table(key)
    cell.map(serializer.deserialize(_).asInstanceOf[JsObject])
  }

  override def apply(key: Key): Option[JsObject] = {
    apply(rowkey(key))
  }

  override def upsert(doc: JsObject): Unit = {
    table(rowkey(doc)) = serializer.serialize(doc)
  }

  override def delete(key: Key): Unit = {
    table.delete(rowkey(key))
  }
}

/** Document collection by BigTable. The key of document serves as the row key in the underlying
  * BigTable implementation. Most BigTable implementations have the upper limit of
  * row key. Although the row key may be as long as 64KB in underlying BigTable,
  * it is always good to keep the row key (and column name) as short as possible.
  *
  * @author Haifeng Li
  */
class BigTableDocuments(val table: BigTable, val rowkey: RowKey) extends Documents {
  /** Document serializer. */
  val serializer = new JsonSerializer()

  /** The table name. */
  override val name = table.name

  override def apply(key: Array[Byte]): Option[JsObject] = {
    val cell = table(key, DocumentColumnFamily, DocumentColumn)
    cell.map(serializer.deserialize(_).asInstanceOf[JsObject])
  }

  override def apply(key: Key): Option[JsObject] = {
    apply(rowkey(key))
  }

  override def upsert(doc: JsObject): Unit = {
    table(rowkey(doc), DocumentColumnFamily, DocumentColumn) = serializer.serialize(doc)
  }

  override def delete(key: Key): Unit = {
    table.delete(rowkey(key))
  }
}
