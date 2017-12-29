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

import unicorn.bigtable._
import unicorn.json._
import unicorn.kv._
import unicorn.unibase.graph._

/** Extending Cabinet, a Unibase supports the data models that require scan operations.
  *
  * @author Haifeng Li
  */
trait Unibase extends Cabinet {
  /** Returns a property graph
    *
    * @param name The name of graph.
    */
  def propertyGraph(name: String): PropertyGraph

  /** Creates a property graph.
    * @param name the name of graph.
    */
  def createPropertyGraph(name: String): Unit

  /** Returns a semantic graph
    *
    * @param name The name of graph.
    */
  def semanticGraph(name: String): SemanticGraph

  /** Creates a semantic graph.
    * @param name the name of graph.
    */
  def createSemanticGraph(name: String): Unit

  /** Creates an index.
    * @param name index name.
    * @param table the data table.
    * @param columns the columns on which the index is built.
    */
  def createIndex(name: String, table: String, columns: (String, Order)*): Unit

  private[unibase] def createIndexMeta(name: String, table: String, columns: (String, Order)*): Unit = {
    val metaOpt = metaTable(table)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"The table $table doesn't exist")

    val meta = metaOpt.get
    if (meta.index(name) != JsUndefined)
      throw new IllegalArgumentException(s"The index $name exists")

    meta.index(name) = TableMeta.rowkey2json(new RowKey(columns))

    metaTable.upsert(meta)
  }
}

object Unibase {
  def apply[T <: OrderedKeyspace](db: KeyValueStore[T]): Unibase = {
    new KeyValueUnibase[T](db)
  }

  def apply[T <: OrderedBigTable](db: BigTableDatabase[T]): Unibase = {
    new BigTableUnibase[T](db)
  }
}

class KeyValueUnibase[+T <: OrderedKeyspace](db: KeyValueStore[T]) extends KeyValueCabinet[T](db) with Unibase {
  override def documents(name: String): Documents with FindOps = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_DOCUMENTS)
      throw new IllegalArgumentException(s"$name is not a drawer")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])

    new KeyValueDocuments(db(name), rowkey) with FindOps {
      override val index: Seq[Index] = meta.index.asInstanceOf[JsObject].fields.map { case (indexName, json) =>
        new KeyValueIndex(db(indexName), TableMeta.json2rowkey(json.asInstanceOf[JsArray]))
      }.toSeq
    }
  }

  override def propertyGraph(name: String): PropertyGraph = {
    new KeyValuePropertyGraph(db(name))
  }

  override def createPropertyGraph(name: String): Unit = {
    db.create(name)
  }

  override def semanticGraph(name: String): SemanticGraph = {
    new KeyValueSemanticGraph(db(name))
  }

  override def createSemanticGraph(name: String): Unit = {
    db.create(name)
  }

  def createIndex(name: String, table: String, columns: (String, Order)*): Unit = {
    createIndexMeta(name, table, columns: _*)
    db.create(name)
  }
}

class BigTableUnibase[+T <: OrderedBigTable](db: BigTableDatabase[T]) extends BigTableCabinet[T](db) with Unibase {
  override def documents(name: String): Documents with FindOps = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_DOCUMENTS)
      throw new IllegalArgumentException(s"$name is not a drawer")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])

    new BigTableDocuments(db(name), rowkey) with FindOps {
      override val index: Seq[Index] = meta.index.asInstanceOf[JsObject].fields.map { case (indexName, json) =>
        new BigTableIndex(db(indexName), TableMeta.json2rowkey(json.asInstanceOf[JsArray]))
      }.toSeq
    }
  }

  override def table(name: String): Table with FindOps = {
    val metaOpt = metaTable(name)
    if (metaOpt.isEmpty)
      throw new IllegalArgumentException(s"$name metadata doesn't exist")

    val meta = metaOpt.get
    if (meta.`type`.toString != TABLE_TYPE_TABLE)
      throw new IllegalArgumentException(s"$name is not a table")

    val rowkey = TableMeta.json2rowkey(meta.key.asInstanceOf[JsArray])

    new Table(db(name), rowkey) with FindOps {
      override val index: Seq[Index] = meta.index.asInstanceOf[JsObject].fields.map { case (indexName, json) =>
        new BigTableIndex(db(indexName), TableMeta.json2rowkey(json.asInstanceOf[JsArray]))
      }.toSeq
    }
  }

  override def propertyGraph(name: String): PropertyGraph = {
    new BigTablePropertyGraph(db(name))
  }

  override def createPropertyGraph(name: String): Unit = {
    db.create(name)
  }

  override def semanticGraph(name: String): SemanticGraph = {
    new BigTableSemanticGraph(db(name))
  }

  override def createSemanticGraph(name: String): Unit = {
    db.create(name)
  }

  def createIndex(name: String, table: String, columns: (String, Order)*): Unit = {
    createIndexMeta(name, table, columns: _*)
    db.create(name)
  }
}