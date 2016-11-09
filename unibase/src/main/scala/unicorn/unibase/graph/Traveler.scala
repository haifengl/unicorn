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

package unicorn.unibase.graph

import unicorn.json._

/** Vertex color mark in a graph graversal. */
object VertexColor extends Enumeration {
  type VertexColor = Value

  /** White marks vertices that have yet to be discovered. */
  val White = Value

  /** Gray marks a vertex that is discovered but still
    * has vertices adjacent to it that are undiscovered. */
  val Gray = Value

  /** A black vertex is discovered vertex that is not
    * adjacent to any white vertices.
    */
  val Black = Value
}

/** The edges to follow in a graph traversal. */
object Direction extends Enumeration {
  type Direction = Value

  /** Outgoing edges. */
  val Outgoing = Value

  /** Incoming edges. */
  val Incoming = Value

  /** Both directions. */
  val Both = Value
}

import VertexColor._

/** Graph traveler is a proxy to the graph during the
  * graph traversal. Beyond the visitor design pattern
  * that process a vertex during the traversal,
  * the traveler also provides the method to access
  * graph vertices, the neighbors of a vertex to explore,
  * and the weight of an edge.
  *
  * @author Haifeng Li
  */
trait Traveler {
  /** Translates a vertex string key to 64 bit id. */
  def idOf(key: String): Option[Long]

  /** Returns the vertex of given ID. */
  def vertex(id: Long): Vertex

  /** Returns the vertex of given string key. */
  def vertex(key: String): Vertex

  /** The color mark if a vertex was already visited. */
  def color(id: Long): VertexColor

  /** Visit a vertex during graph traversal.
    *
    * @param vertex the vertex on visiting.
    * @param edge the incoming arc (None for starting vertex).
    * @param hops the number of hops from the starting vertex to this vertex.
    */
  def visit(vertex: Vertex, edge: Option[Edge], hops: Int): Unit

  /** Returns an iterator of the neighbors and associated edges of a vertex.
    *
    * @param vertex the vertex on visiting.
    * @param hops the number of hops from starting vertex, which may be used for early termination.
    * @return an iterator of the outgoing edges
    */
  def neighbors(vertex: Vertex, hops: Int): Iterator[(Long, Edge)]

  /** The weight of edge (e.g. shortest path search). */
  def weight(edge: Edge): Double = edge.properties match {
    case JsInt(x) => x
    case JsCounter(x) => x
    case JsLong(x) => x
    case _ => 1.0
  }
}

/** Traveler for A* searcher. */
trait AstarTraveler extends Traveler {
  /** The future path-cost function, which is an admissible
    * "heuristic estimate" of the distance from the current vertex to the goal.
    * Note that the heuristic function must be monotonic.
    */
  def h(v1: Long, v2: Long): Double
}