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

import unicorn.unibase.Key

/** Simple graph visitor with cache management.
  * In DFS and BFS, the user should create a sub class overriding
  * the `apply` method, which is nop by default.
  *
  * @param graph The graph to visit.
  * @param maxHops Maximum number of hops during graph traversal.
  * @param p Optional filter on relationships of interest. Only
  *          neighbors with given relationship will be visited.
  *
  * @author Haifeng Li
  */
class SimpleTraveler[V <: VertexLike, E <: EdgeLike](val graph: GraphLike[V, E], val maxHops: Int = 5, val p: Option[E => Boolean]) extends Traveler[V, E] {
  /** The color mark if a vertex was already visited. */
  private val mark = collection.mutable.Map[Key, VertexColor]().withDefaultValue(White)

  /** The cache of vertices. */
  private val cache = collection.mutable.Map[Key, Seq[E]]()

  override def apply(vertex: Key): Option[V] = {
    graph(vertex)
  }

  /** User defined vertex visit function. The default implementation is nop.
    * The user should create a sub class overriding this method.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (None for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def apply(vertex: Key, edge: Option[E], hops: Int): Unit = {

  }

  /** Resets the vertex color to unvisited and clean up the cache. */
  def reset: Unit = {
    mark.clear
    cache.clear
  }

  override def color(vertex: Key): VertexColor = mark(vertex)

  override def visit(vertex: Key, edge: Option[E], hops: Int): Unit = {
    apply(vertex, edge, hops)

    val black = graph.edges(vertex).forall { neighbor =>
      mark.contains(neighbor.to)
    }

    mark(vertex) = if (black) Black else Gray
  }

  override def edges(vertex: Key, hops: Int): Iterator[E] = {
    if (hops >= maxHops) return Seq.empty.iterator

    val edges = graph.edges(vertex)
    if (p.isDefined) edges.filter(p.get) else edges
  }

  override def weight(edge: E): Double = 1.0
}
