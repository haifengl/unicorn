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

/** Simple graph visitor with cache management.
  * In DFS and BFS, the user should create a sub class overriding
  * the `apply` method, which is nop by default.
  *
  * @param graph The graph to visit.
  * @param relationships Relationship of interest. Only neighbors with given
  *                      relationship will be visited. Empty set means all
  *                      relationships.
  * @param maxHops Maximum number of hops during graph traversal.
  * @param direction Edges to follow in the traversal.
  *
  * @author Haifeng Li
  */
class SimpleTraveler[T, VI <: VertexId[T], V <: VertexId[T], E <: EdgeLike[T, V]](val graph: GraphLike[T, VI, V, E], val relationships: Set[String] = Set.empty, val maxHops: Int = 3, val direction: Direction = Outgoing) extends Traveler[T, VI, V, E] {
  /** The color mark if a vertex was already visited. */
  private val mark = collection.mutable.Map[V, VertexColor]().withDefaultValue(White)

  /** The cache of vertices. */
  private val cache = collection.mutable.Map[V, Seq[E]]()

  /** User defined vertex visit function. The default implementation is nop.
    * The user should create a sub class overriding this method.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (None for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def apply(vertex: V, edge: Option[E], hops: Int): Unit = {

  }

  /** Resets the vertex color to unvisited and clean up the cache. */
  def reset: Unit = {
    mark.clear
    cache.clear
  }
/*
  override def vertex(id: Long): Vertex = {
    cache.get(id) match {
      case Some(node) => node
      case None =>
        val node = graph(id, direction)
        cache(id) = node
        node
    }
  }

  override def vertex(key: String): Vertex = {
    val id = idOf(key)
    require(id.isDefined, s"Vertex $key doesn't exist")
    vertex(id.get)
  }

  /** Translates a vertex string key to 64 bit id. */
  override def idOf(key: String): Option[Long] = {
    graph.idOf(key)
  }
*/
  override def color(vertex: V): VertexColor = mark(vertex)

  override def visit(vertex: V, edge: Option[E], hops: Int): Unit = {
    /*
    apply(vertex, edge, hops)

    val black = vertex.neighbors.forall { neighbor =>
      mark.contains(neighbor)
    }

    mark(vertex.id) = if (black) Black else Gray
    */
  }

  override def neighbors(vertex: V, hops: Int): Iterator[E] = {
    /*
    if (hops >= maxHops) return Seq.empty.iterator

    val edges = if (relationships.isEmpty) vertex.edges
    else vertex.edges.filter { edge =>
      relationships.contains(edge.label)
    }

    edges.map { edge =>
      val neighbor = if (edge.to != vertex.id) edge.to else edge.from
      (neighbor, edge)
    }.iterator
    */
  }
}
