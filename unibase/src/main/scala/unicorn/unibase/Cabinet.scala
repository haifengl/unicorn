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

import unicorn.bigtable.{BigTable, BigTableDatabase}
import unicorn.json._
import unicorn.kv.{Keyspace, KeyValue, KeyValueStore}

/** A Cabinet is a database of documents. A collection of documents can be
  * stored as a Drawer or Table. Drawer uses a compact storage format but
  * can only get and save a document as the whole. In contrast, Table
  * allows us to get/update some top-level fields of documents.
  *
  * @author Haifeng Li
  */
trait Cabinet {
  private[unibase] val metaTable: Documents

  /** Returns a document collection.
    * @param name The name of document collection.
    */
  def documents(name: String): Documents

  /** Creates a document collection.
    * @param name the name of document collection.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  def createDocuments(name: String, key: RowKey = RowKey(DefaultRowKeyField)): Unit

  /** Drops a table. */
  def drop(name: String): Unit

  /** Tests if a table exists.
    * @param name the name of table.
    */
  def exists(name: String): Boolean

  /** Returns the list of BigTables. */
  def tables: Set[String]
}

object Cabinet {
  def apply[T <: Keyspace](db: KeyValueStore[T]): KeyValueCabinet[T] = {
    new KeyValueCabinet[T](db)
  }

  def apply[T <: BigTable](db: BigTableDatabase[T]): BigTableCabinet[T] = {
    new BigTableCabinet[T](db)
  }
}

class KeyValueCabinet[+T <: Keyspace](db: KeyValueStore[T]) extends Cabinet {

  private[unibase] lazy override val metaTable = {
    if (!db.exists(MetaTableName)) {
      db.create(MetaTableName)
    }

    new KeyValueDocuments(db(MetaTableName), RowKey("table"))
  }

  /** Returns a document collection.
    * @param name The name of document collection.
    */
  override def documents(name: String): Documents = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_DOCUMENTS)
      throw new IllegalArgumentException(s"$name is not a drawer")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])
    new KeyValueDocuments(db(name), rowkey)
  }

  /** Creates a document collection.
    * @param name the name of document collection.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  override def createDocuments(name: String, key: RowKey = RowKey(DefaultRowKeyField)): Unit = {
    db.create(name)

    val meta = TableMeta(name, TABLE_TYPE_DOCUMENTS, key)
    metaTable.upsert(meta)
  }

  /** Drops a table. All column families in the table will be dropped. */
  override def drop(name: String): Unit = {
    db.drop(name)
    metaTable.delete(name)
  }

  /** Tests if a BigTable exists.
    * @param name the name of table.
    */
  override def exists(name: String): Boolean = {
    db.exists(name)
  }

  /** Returns the list of BigTables. */
  override def tables: Set[String] = {
    db.tables
  }
}

class BigTableCabinet[+T <: BigTable](db: BigTableDatabase[T]) extends Cabinet {

  private[unibase] lazy override val metaTable = {
    if (!db.exists(MetaTableName)) {
      db.create(MetaTableName, DocumentColumnFamily)
    }

    new BigTableDocuments(db(MetaTableName), RowKey("table"))
  }

  /** Returns a document collection.
    * @param name The name of document collection.
    */
  override def documents(name: String): Documents = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_DOCUMENTS)
      throw new IllegalArgumentException(s"$name is not a drawer")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])
    new BigTableDocuments(db(name), rowkey)
  }

  /** Returns a document collection.
    * @param name The name of document collection.
    */
  def table(name: String): Table = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_TABLE)
      throw new IllegalArgumentException(s"$name is not a table")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])
    new Table(db(name), rowkey)
  }

  /** Creates a document collection.
    * @param name the name of document collection.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  override def createDocuments(name: String, key: RowKey = RowKey(DefaultRowKeyField)): Unit = {
    db.create(name, DocumentColumnFamily)

    val meta = TableMeta(name, TABLE_TYPE_DOCUMENTS, key)
    metaTable.upsert(meta)
  }

  /** Creates a document table.
    * @param name the name of table.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  def createTable(name: String, key: RowKey = RowKey(DefaultRowKeyField)): Unit = {
    db.create(name, DocumentColumnFamily)

    val meta = TableMeta(name, TABLE_TYPE_TABLE, key)
    metaTable.upsert(meta)
  }

  /** Drops a table. All column families in the table will be dropped. */
  override def drop(name: String): Unit = {
    db.drop(name)
    metaTable.delete(name)
  }

  /** Truncates a BigTable.
    * @param name the name of table.
    */
  def truncate(name: String): Unit = {
    db.truncate(name)
  }

  /** Tests if a BigTable exists.
    * @param name the name of table.
    */
  override def exists(name: String): Boolean = {
    db.exists(name)
  }

  /** Major compacts a BigTable. Asynchronous operation.
    * @param name the name of table.
    */
  def compact(name: String): Unit = {
    db.compact(name)
  }

  /** Returns the list of BigTables. */
  override def tables: Set[String] = {
    db.tables
  }
}

private[unicorn] object TableMeta {
  /** Creates JsObject of table meta data.
    *
    * @param table The name of table.
    * @param `type` The type of table.
    * @param key The document field(s) used as row key in BigTable.
    * @return JsObject of meta data.
    */
  def apply(table: String, `type`: String, key: RowKey): JsObject = {
    JsObject("table" -> table, "type" -> `type`, "key" -> rowkey2json(key), "index" -> JsObject())
  }

  /** Converts RowKey to Json. */
  def rowkey2json(key: RowKey): JsArray = {
    JsArray(
      key.fields.map { case (key, order) =>
        JsObject(key -> JsString(order.toString))
      }
    )
  }

  /** Creates the RowKey from the meta data.
    * @param key The meta data of row key.
    */
  def json2rowkey(key: JsArray): RowKey = {
    new RowKey(key.elements.map { element =>
      val field = element.asInstanceOf[JsObject].fields.toSeq(0)
      // Maybe this is a bug of Scala compiler. It doesn't recognize Order type alias.
      (field._1, org.apache.hadoop.hbase.util.Order.valueOf(field._2))
    })
  }
}