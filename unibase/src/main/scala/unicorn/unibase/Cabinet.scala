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

import unicorn.bigtable.{BigTable, Database}
import unicorn.json._

/** A Cabinet is a database of documents. A collection of documents can be
  * stored as a Drawer or Table. Drawer uses a compact storage format but
  * can only get and save a document as the whole. In contrast, Table
  * allows us to get/update only some (top) fields of documents.
  * In addition to documents, Cabinet also supports the graph model.
  *
  * @author Haifeng Li
  */
class Cabinet[+T <: BigTable](db: Database[T]) {

  private lazy val metaTable = {
    if (!db.tableExists(MetaTableName)) {
      val metaTable = db.createTable(MetaTableName, DocumentColumnFamily)
      metaTable.close
    }

    new Table(db(MetaTableName), RowKey("table"))
  }

  /** Returns a document drawer.
    * @param name The name of table.
    */
  def drawer(name: String): Drawer = {
    val meta = metaTable(name)
    if (meta.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    if (meta.get.`type`.toString != TABLE_TYPE_DRAWER)
      throw new IllegalArgumentException(s"$name is not a drawer")

    val rowkey = meta.map(TableMeta.rowkey(_)).get
    new Drawer(db(name), rowkey)
  }

  /** Returns a document table.
    * @param name The name of table.
    */
  def table(name: String): Table = {
    val meta = metaTable(name)
    if (meta.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    if (meta.get.`type`.toString != TABLE_TYPE_TABLE)
      throw new IllegalArgumentException(s"$name is not a table")

    val rowkey = meta.map(TableMeta.rowkey(_)).get
    new Table(db(name), rowkey)
  }
/*
  /** Returns a read only graph, which doesn't need an ID
    * generator. This is sufficient for graph traversal and analytics.
    *
    * @param name The name of graph table.
    */
  def graph(name: String): Graph = {
    new Graph(db(name), db(graphVertexKeyTable(name)))
  }
*/
  /** Creates a document drawer.
    * @param name the name of table.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  def createDrawer(name: String,
                   key: RowKey = PrimitiveRowKey(DefaultRowKeyField)): Unit = {
    val table = db.createTable(name, DocumentColumnFamily)
    // RocksDB will hold the lock if we don't close it
    table.close

    val meta = TableMeta(name, TABLE_TYPE_DRAWER, key)
    metaTable.upsert(meta)
  }

  /** Creates a document table.
    * @param name the name of table.
    * @param key the document field(s) used as row key in BigTable.
    *            If not specified, the "_id" field is used as the
    *            document key as in MongoDB.
    */
  def createTable(name: String,
                   key: RowKey = PrimitiveRowKey(DefaultRowKeyField)): Unit = {
    val table = db.createTable(name, DocumentColumnFamily)
    // RocksDB will hold the lock if we don't close it
    table.close

    val meta = TableMeta(name, TABLE_TYPE_TABLE, key)
    metaTable.upsert(meta)
  }
/*
  /** Creates a graph table.
    * @param name the name of graph table.
    */
  def createGraph(name: String): Unit = {
    val vertexKeyTable = graphVertexKeyTable(name)
    require(!db.tableExists(vertexKeyTable), s"Vertex key table $vertexKeyTable already exists")

    val table = db.createTable(name,
      GraphVertexColumnFamily,
      GraphInEdgeColumnFamily,
      GraphOutEdgeColumnFamily)
    table.close

    val keyTable = db.createTable(vertexKeyTable, GraphVertexColumnFamily)
    keyTable.close
  }
*/
  /** Drops a table. All column families in the table will be dropped. */
  def drop(name: String): Unit = {
    db.dropTable(name)
    metaTable.delete(name)
  }

  /** Truncates a BigTable.
    * @param name the name of table.
    */
  def truncate(name: String): Unit = {
    db.truncateTable(name)
  }

  /** Tests if a BigTable exists.
    * @param name the name of table.
    */
  def exists(name: String): Boolean = {
    db.tableExists(name)
  }

  /** Major compacts a BigTable. Asynchronous operation.
    * @param name the name of table.
    */
  def compact(name: String): Unit = {
    db.compactTable(name)
  }

  /** Returns the list of BigTables. */
  def tables: Set[String] = {
    db.tables
  }
}

object Cabinet {
  def apply[T <: BigTable](db: Database[T]): Cabinet[T] = {
    new Cabinet[T](db)
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
    val rowkey = key match {
      case PrimitiveRowKey(key, order) => JsObject(key -> JsString(order.toString))
      case CompoundRowKey(keys, capacity) =>
        JsObject(
          "compound_key" -> JsArray(keys.map { case PrimitiveRowKey(key, order) =>
              JsObject(key -> JsString(order.toString))
            }),
          "capacity" -> capacity
        )
    }

    JsObject("table" -> table, "type" -> `type`, "key" -> rowkey)
  }

  /** Creates the RowKey from the meta data.
    * @param meta The meta data of table.
    */
  def rowkey(meta: JsValue): RowKey = {
    val key = meta.key
    key.compound_key match {
      case JsUndefined => primitive(key)

      case JsArray(keys) =>
        val capacity: Int = key.capacity
        CompoundRowKey(keys.map { key => primitive(key) }, capacity)

      case _ => throw new IllegalArgumentException(s"Invalid row key configuration in metadata: $key")
    }
  }

  /** Creates the primitive key. */
  private def primitive(key: JsValue): PrimitiveRowKey = {
    if (!key.isInstanceOf[JsObject])
      throw new IllegalArgumentException(s"Invalid row key configuration in metadata: $key")

    val fields = key.asInstanceOf[JsObject].fields.toSeq
    if (fields.size != 1)
      throw new IllegalArgumentException(s"Invalid row key configuration in metadata: $key")

    val field = fields(0)
    // Maybe this is a bug of Scala compiler. It doesn't recognize Order type alias.
    PrimitiveRowKey(field._1, org.apache.hadoop.hbase.util.Order.valueOf(field._2))
  }
}