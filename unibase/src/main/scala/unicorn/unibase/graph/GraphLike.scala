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
  * @author Haifeng Li
  */
trait GraphLike[T, VI <: VertexId[T], V <: VertexLike[T], E <: EdgeLike[T, VI]] {

  /** Gets a vertex. */
  def apply(vertex: T): Option[V]

  /** Gets a vertex. */
  def apply(vertex: VI): Option[V]

  /** Returns the edges of a given vertex. */
  def edges(vertex: T): Iterator[E]

  /** Returns the edges of a given vertex. */
  def edges(vertex: VI): Iterator[E]

  /** Returns the edges of given type. */
  def edges(vertex: T, `type`: String): Iterator[E]

  /** Returns the edges of given type. */
  def edges(vertex: VI, `type`: String): Iterator[E]

  /** Adds an edge. If the edge exists, the associated data will be overwritten. */
  def add(edge: E): Unit

  /** Deletes an edge. */
  def delete(edge: E): Unit
}
