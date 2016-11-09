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

package unicorn.unibase

import unicorn.bigtable.{Column, BigTable, Database}
import unicorn.unibase.graph.Graph
import unicorn.json._
import unicorn.util._

/** A Unibase is a database of documents. A collection of documents are called table.
  *
  * @author Haifeng Li
  */
class Unibase[+T <: BigTable](db: Database[T]) {
  import unicorn.unibase.graph.GraphVertexKeyTableSuffix
  import unicorn.unibase.graph.GraphVertexColumnFamily
  import unicorn.unibase.graph.GraphInEdgeColumnFamily
  import unicorn.unibase.graph.GraphOutEdgeColumnFamily

  private[unicorn] def graphVertexKeyTable(name: String): String = {
    name + GraphVertexKeyTableSuffix
  }

  /** Returns a document table.
    * @param name The name of table.
    */
  def apply(name: String): Table = {
    new Table(db(name), TableMeta(db, name))
  }

  /** Returns a read only graph, which doesn't need an ID
    * generator. This is sufficient for graph traversal and analytics.
    *
    * @param name The name of graph table.
    */
  def graph(name: String): Graph = {
    new Graph(db(name), db(graphVertexKeyTable(name)))
  }

  /** Creates a document table.
    * @param name the name of table.
    * @param families the column family that documents resident.
    * @param locality a map of document fields to column families for storing of sets of fields in column families
    *                 separately to allow clients to scan over fields that are frequently used together efficient
    *                 and to avoid scanning over column families that are not requested.
    */
  def createTable(name: String,
                  families: Seq[String] = Seq(DocumentColumnFamily),
                  locality: Map[String, String] = Map().withDefaultValue(DocumentColumnFamily),
                  appendOnly: Boolean = false): Unit = {
    val table = db.createTable(name, families: _*)
    // RocksDB will hold the lock if we don't close it
    table.close

    // If the meta data table doesn't exist, create it.
    if (!db.tableExists(MetaTableName)) {
      val metaTable = db.createTable(MetaTableName, MetaTableColumnFamily)
      metaTable.close
    }

    // meta data table
    val metaTable = db(MetaTableName)
    val serializer = new ColumnarJsonSerializer
    val meta = TableMeta(families, locality, appendOnly)
    val columns = serializer.serialize(meta).map {
      case (path, value) => Column(path.getBytes(utf8), value)
    }.toSeq
    metaTable.put(name, MetaTableColumnFamily, columns: _*)
    metaTable.close
  }

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

  /** Drops a document table. All column families in the table will be dropped. */
  def dropTable(name: String): Unit = {
    db.dropTable(name)
    db(MetaTableName).delete(name)
  }

  /** Drops a graph. All tables related to the graph will be dropped. */
  def dropGraph(name: String): Unit = {
    db.dropTable(name)
    db.dropTable(graphVertexKeyTable(name))
  }

  /** Truncates a table
    * @param name the name of table.
    */
  def truncateTable(name: String): Unit = {
    db.truncateTable(name)
  }

  /** Tests if a table exists.
    * @param name the name of table.
    */
  def tableExists(name: String): Boolean = {
    db.tableExists(name)
  }

  /** Major compacts a table. Asynchronous operation.
    * @param name the name of table.
    */
  def compactTable(name: String): Unit = {
    db.compactTable(name)
  }

  /** Returns the list of tables. */
  def tables: Set[String] = {
    db.tables.filter(!_.endsWith(GraphVertexKeyTableSuffix))
  }
}

object Unibase {
  def apply[T <: BigTable](db: Database[T]): Unibase[T] = {
    new Unibase[T](db)
  }
}

private[unicorn] object TableMeta {
  /** Creates JsObject of table meta data.
    *
    * @param families Column families of document store. There may be other column families in the underlying table
    *                 for meta data or index.
    * @param locality Locality map of document fields to column families.
    * @param appendOnly True if the table is append only.
    * @return JsObject of meta data.
    */
  def apply(families: Seq[String], locality: Map[String, String], appendOnly: Boolean): JsObject = {
    JsObject(
      "families" -> families.toJsArray,
      "locality" -> locality.toJsObject,
      DefaultLocalityField -> locality(""), // hacking the default value of a map
      "appendOnly" -> appendOnly
    )
  }

  /** Retrieves the meta data of a table.
    * @param db the host database.
    * @param name table name.
    * @return JsObject of table meta data.
    */
  def apply(db: Database[BigTable], name: String): JsObject = {
    val metaTable = db(MetaTableName)
    val serializer = new ColumnarJsonSerializer
    val meta = metaTable.get(name, MetaTableColumnFamily).map {
      case Column(qualifier, value, _) => (new String(qualifier, utf8), value.bytes)
    }.toMap
    metaTable.close
    serializer.deserialize(meta).asInstanceOf[JsObject]
  }
}