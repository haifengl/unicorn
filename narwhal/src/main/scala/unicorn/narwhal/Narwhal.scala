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

package unicorn.narwhal

import unicorn.bigtable.hbase.HBase
import unicorn.unibase.{TableMeta, Unibase}
import unicorn.narwhal.graph.GraphX

/** Unibase specialized for HBase. */
class Narwhal(hbase: HBase) extends Unibase(hbase) {
  /** Returns a document table.
    * @param name the name of table.
    */
  override def apply(name: String): HTable = {
    new HTable(hbase(name), TableMeta(hbase, name))
  }

  /** The connection string of ZooKeeper instance used by this HBase. */
  val zookeeper = hbase.connection.getConfiguration.get("hbase.zookeeper.quorum")

  override def graph(name: String): GraphX = {
    new GraphX(hbase(name), hbase(graphVertexKeyTable(name)))
  }

/*
  /** Returns a document table.
    * @param name the name of table.
    */
  def getTableWithIndex(name: String): HTableWithIndex = {
    new HTableWithIndex(hbase.getTableWithIndex(name), TableMeta(hbase, name))
  }
  */
}

object Narwhal {
  def apply(): Narwhal = {
    new Narwhal(HBase())
  }

  def apply(db: HBase): Narwhal = {
    new Narwhal(db)
  }
}