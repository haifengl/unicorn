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

/** Vertex color mark in a graph graversal. */
sealed trait VertexColor

/** White marks vertices that have yet to be discovered. */
case object White extends VertexColor

/** Gray marks a vertex that is discovered but still
  * has vertices adjacent to it that are undiscovered. */
case object Gray extends VertexColor

/** A black vertex is discovered vertex that is not
  * adjacent to any white vertices.
  */
case object Black extends VertexColor


/** The edges to follow in a graph traversal. */
sealed trait Direction

/** Outgoing edges. */
case object Outgoing extends Direction

/** Incoming edges. */
case object Incoming extends Direction

/** Both directions. */
case object Both extends Direction

/** Graph traveler is a proxy to the graph during the
  * graph traversal. Beyond the visitor design pattern
  * that process a vertex during the traversal,
  * the traveler also provides the method to access
  * graph vertices, the neighbors of a vertex to explore,
  * and the weight of an edge.
  *
  * @author Haifeng Li
  */
trait Traveler[V <: VertexLike, E <: EdgeLike] {
  /** Returns the vertex of given ID. */
  def apply(vertex: Key): Option[V]

  /** The color mark if a vertex was already visited. */
  def color(vertex: Key): VertexColor

  /** Visit a vertex during graph traversal.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (None for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def visit(vertex: Key, edge: Option[E], hops: Int): Unit

  /** Returns an iterator of the (outgoing) edges of a vertex.
    *
    * @param vertex the vertex on visiting.
    * @param hops the number of hops from starting vertex, which may be used for early termination.
    * @return an iterator of the outgoing edges
    */
  def edges(vertex: Key, hops: Int): Iterator[E]

  /** The weight of edge (e.g. shortest path search). */
  def weight(edge: E): Double = 1.0

  /** The future path-cost function in A* search, which is an admissible
    * "heuristic estimate" of the distance from the current vertex to the goal.
    * Note that the heuristic function must be monotonic.
    */
  def h(current: Key, goal: Key): Double = 1.0
}