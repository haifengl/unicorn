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

import unicorn.bigtable.{RowScan, Column, BigTable, Database}
import unicorn.unibase.graph.Graph
//import unicorn.unibase.graph.{Graph, KnowledgeGraph}
import unicorn.json._
import unicorn.util._

/** Extending Cabinet, a Unibase supports the data models that require scan operations.
  *
  * @author Haifeng Li
  */
class Unibase[+T <: BigTable with RowScan](db: Database[T]) extends Cabinet(db) {
  import unicorn.unibase.graph.{KnowledgeGraphOSP, KnowledgeGraphPOS, KnowledgeGraphSPO}

  /** Returns a document table.
    * @param name The name of table.
    */
  override def apply(name: String): Table = {
    new Table(db(name), TableMeta(db, name))
  }
/*
  /** Creates a knowledge graph table.
    * @param name the name of graph.
    */
  def createKnowledgeGraph(name: String): Unit = {
    val vertexKeyTable = graphVertexKeyTable(name)
    require(!db.tableExists(vertexKeyTable), s"Vertex key table $vertexKeyTable already exists")

    val spo = db.createTable(name + KnowledgeGraphSPO)
    spo.close

    val osp = db.createTable(name + KnowledgeGraphOSP)
    osp.close

    val pos = db.createTable(name + KnowledgeGraphPOS)
    pos.close
  }

  /** Drops a knowledge graph. All tables related to the graph will be dropped. */
  def dropKnowledgeGraph(name: String): Unit = {
    db.dropTable(name + KnowledgeGraphSPO)
    db.dropTable(name + KnowledgeGraphOSP)
    db.dropTable(name + KnowledgeGraphPOS)
  }

  def knowledge(name: String): KnowledgeGraph = {

  }
  */
}

object Unibase {
  def apply[T <: BigTable with RowScan](db: Database[T]): Unibase[T] = {
    new Unibase[T](db)
  }
}

