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

package unicorn.unibase.graph

import com.typesafe.scalalogging.Logger
import unicorn.bigtable.{BigTable, Column, RowScan}
import unicorn.json._

/** Graphs are mathematical structures used to model pairwise relations
  * between objects. A graph is made up of vertices (nodes) which are
  * connected by edges (arcs or lines). A graph may be undirected, meaning
  * that there is no distinction between the two vertices associated with
  * each edge, or its edges may be directed from one vertex to another.
  * Directed graphs are also called digraphs and directed edges are also
  * called arcs or arrows.
  *
  * A multigraph is a graph which is permitted to have multiple edges
  * (also called parallel edges), that is, edges that have the same end
  * nodes. The ability to support parallel edges simplifies modeling
  * scenarios where there can be multiple relationships (e.g., co-worker
  * and friend) between the same vertices.
  *
  * In a property graph, the generic mathematical graph is often extended
  * to support user defined objects attached to each vertex and edge.
  * The edges also have associated labels denoting the relationships,
  * which are important in a multigraph.
  *
  * Unicorn supports directed property multigraphs. Each relationship/edge
  * has a label and optional data (any valid JsValue, default value JsInt(1)).
  *
  * Unicorn stores graphs in adjacency lists. That is, a graph
  * is stored as a BigTable whose rows are vertices with their adjacency list.
  * The adjacency list of a vertex contains all of the vertex’s incident edges
  * (in and out edges are in different column families).
  *
  * Because large graphs are usually very sparse, an adjacency list is
  * significantly more space-efficient than an adjacency matrix.
  * Besides, the neighbors of each vertex may be listed efficiently with
  * an adjacency list, which is important in graph traversals.
  * With our design, it is also possible to
  * test whether two vertices are adjacent to each other
  * for a given relationship in constant time.
  *
  * @author Haifeng Li
  */
class GraphLike[+V <: VertexLike, +E <: EdgeLike[V]](val table: BigTable with RowScan) {
  lazy val logger = Logger(getClass)
  /** Edge property serializer. */
  val serializer = new JsonSerializer()

  /*
  /** Returns a Gremlin traversal machine. */
  def traversal: Gremlin = {
    new Gremlin(new SimpleTraveler(this, direction = Direction.Both))
  }

  /** Returns a Gremlin traversal machine starting at the given vertex. */
  def apply(vertex: V): GremlinVertices = {
    val g = traversal
    g.v(vertex)
  }
  */

  /** Adds an edge. If the edge exists, the associated data will be overwritten. */
  def add(edge: E): Unit = {
    /*
    val fromKey = serializer.serialize(from)
    val toKey = serializer.serialize(to)

    val columnPrefix = serializer.edgeSerializer.str2Bytes(label)
    val value = serializer.serializeEdge(properties)

    table.put(fromKey, GraphOutEdgeColumnFamily, Column(serializer.serializeEdgeColumnQualifier(columnPrefix, to), value))
    table.put(toKey, GraphInEdgeColumnFamily, Column(serializer.serializeEdgeColumnQualifier(columnPrefix, from), value))
    */
  }

  /** Deletes an edge. */
  def delete(edge: E): Unit = {
    /*
    val fromKey = serializer.serialize(from)
    val toKey = serializer.serialize(to)
    val columnPrefix = serializer.edgeSerializer.str2Bytes(label)

    table.delete(fromKey, GraphOutEdgeColumnFamily, serializer.serializeEdgeColumnQualifier(columnPrefix, to))
    table.delete(toKey, GraphInEdgeColumnFamily, serializer.serializeEdgeColumnQualifier(columnPrefix, from))
    */
  }
}
