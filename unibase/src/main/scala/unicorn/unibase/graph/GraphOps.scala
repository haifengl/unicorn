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

/** Advanced graph operations.
  *
  * @author Haifeng Li
  */
object GraphOps {

  type Path = List[EdgeLike]

  /** Depth-first search of graph.
    * @param vertex the current vertex to visit
    * @param edge optional incoming edge
    * @param traveler the graph traveler proxy.
    * @param hops the number of hops to reach this vertex from the starting vertex.
    */
  private def dfs[V <: VertexLike, E <: EdgeLike](vertex: Key, edge: Option[E], traveler: Traveler[V, E], hops: Int): Unit = {
    traveler.visit(vertex, edge, hops)
    traveler.edges(vertex, hops).foreach { case edge =>
      if (traveler.color(edge.to) == White)
        dfs(edge.to, Some(edge), traveler, hops + 1)
    }
  }

  /** Depth-first search of graph.
    * @param start the starting vertex
    * @param traveler the graph traveler proxy.
    */
  def dfs[V <: VertexLike, E <: EdgeLike](start: Key, traveler: Traveler[V, E]): Unit = {
    dfs(start, None, traveler, 0)
  }

  /** Breadth-first search of graph.
    * @param start the start vertex to visit
    * @param traveler the graph traveler proxy.
    */
  def bfs[V <: VertexLike, E <: EdgeLike](start: Key, traveler: Traveler[V, E]): Unit = {
    val queue = collection.mutable.Queue[(Key, Option[E], Int)]()

    queue.enqueue((start, None, 0))

    while (!queue.isEmpty) {
      val (vertex, edge, hops) = queue.dequeue
      if (traveler.color(vertex) == White) {
        traveler.visit(vertex, edge, hops)
        traveler.edges(vertex, hops).foreach { case edge =>
          queue.enqueue((edge.to, Some(edge), hops + 1))
        }
      }
    }
  }

  /** Helper ordering object in A* for priority queue. */
  private object NodeOrdering extends scala.math.Ordering[(Key, Double, Int)] {
    def compare(x: (Key, Double, Int), y: (Key, Double, Int)): Int = {
      x._2.compare(y._2)
    }
  }

  /** Dijkstra shortest path search algorithm.
    *
    * @param start  the start vertex
    * @param goal   the goal vertex
    * @param traveler the graph traveler proxy.
    * @return       the path from start to goal
    */
  def dijkstra[V <: VertexLike, E <: EdgeLike](start: Key, goal: Key, traveler: Traveler[V, E]): Path = {

    val queue = new scala.collection.mutable.PriorityQueue[(Key, Double, Int)]()(NodeOrdering)
    queue.enqueue((start, 0.0, 0))

    val dist = scala.collection.mutable.Map[Key, Double]().withDefaultValue(Double.PositiveInfinity)
    dist(start) = 0.0

    // The map of navigated vertices
    val cameFrom = scala.collection.mutable.Map[Key, (Key, E)]()

    while (!queue.isEmpty) {
      val (current, distance, hops) = queue.dequeue

      if (current == goal)
        return reconstructPath(cameFrom, goal).reverse

      traveler.edges(current, hops).foreach { edge =>
        val neighbor = edge.to
        val alt = distance + traveler.weight(edge)
        if (alt < dist(neighbor)) {
          dist(neighbor) = alt
          cameFrom(neighbor) = (current, edge)
          queue.enqueue((neighbor, alt, hops + 1))
        }
      }
    }

    // Fail. No path exists between the start vertex and the goal.
    return List.empty
  }

  /** A* search algorithm for path finding and graph traversal.
    * It is an extension of Dijkstra algorithm and achieves better performance by using heuristics.
    *
    * @param start  the start vertex
    * @param goal   the goal vertex
    * @param traveler the graph traveler proxy.
    */
  def astar[V <: VertexLike, E <: EdgeLike](start: Key, goal: Key, traveler: Traveler[V, E]): Path = {
    val guess = traveler.h(start, goal)

    // The queue to find vertex with lowest f score
    // Note that Scala priority queue maintains largest value on the top.
    // So we will use negative f score in the queue.
    val openQueue = new scala.collection.mutable.PriorityQueue[(Key, Double, Int)]()(NodeOrdering)
    openQueue.enqueue((start, -guess, 0))

    // The set of tentative vertices to be evaluated.
    val openSet = scala.collection.mutable.Set[Key](start)

    // The set of vertices already evaluated.
    val closedSet = scala.collection.mutable.Set[Key]()

    // The map of navigated vertices
    val cameFrom = scala.collection.mutable.Map[Key, (Key, E)]()

    // Cost from start along best known path.
    val gScore = scala.collection.mutable.Map[Key, Double]()
    gScore(start) = 0.0

    // Estimated total cost from start to goal through y.
    val fScore = scala.collection.mutable.Map[Key, Double]()
    fScore(start) = guess

    while (!openQueue.isEmpty) {
      val (current, _, hops) = openQueue.dequeue

      if (current == goal)
        return reconstructPath(cameFrom, goal).reverse

      openSet.remove(current)
      closedSet.add(current)

      traveler.edges(current, hops).foreach { edge =>
        val neighbor = edge.to
        if (!closedSet.contains(neighbor)) {
          val alt = gScore(current) + traveler.weight(edge)

          if (!openSet.contains(neighbor) || alt < gScore(neighbor)) {
            cameFrom(neighbor) = (current, edge)
            gScore(neighbor) = alt
            val f = -gScore(neighbor) - traveler.h(neighbor, goal)
            fScore(neighbor) = f
            if (!openSet.contains(neighbor)) {
              openSet.add(neighbor)
              openQueue.enqueue((neighbor, f, hops + 1))
            }
          }
        }
      }
    }

    // Fail. No path exists between the start vertex and the goal.
    return List.empty
  }

  /** Reconstructs the A* search path. */
  private def reconstructPath[E <: EdgeLike](cameFrom: scala.collection.mutable.Map[Key, (Key, E)], current: Key): Path = {
    if (cameFrom.contains(current)) {
      val (from, edge) = cameFrom(current)
      edge :: reconstructPath(cameFrom, from)
    } else {
      List.empty
    }
  }
}
