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

import scala.language.dynamics
import unicorn.json.{JsArray, JsObject, JsValue}

/** The Gremlin Graph Traversal Machine and Language
  *
  * @author Haifeng Li
  */
class Gremlin[T, VI <: VertexId[T], V <: VertexLike[T], E <: EdgeLike[T, VI]](val traveler: Traveler[T, VI, V, E]) {
  /*
  def v(id: Long): GremlinVertices = new GremlinVertices(traveler, Seq(id))
  def v(id: Array[Long]): GremlinVertices = new GremlinVertices(traveler, id)
  def v(key: String*): GremlinVertices = {
    val id = key.map { key =>
      val v = traveler.idOf(key)
      require(v.isDefined, s"Vertex $key doesn't exist")
      v.get
    }
    new GremlinVertices(traveler, id)
  }
  */
}

/*
class GremlinVertices[T, V <: VertexId[T], E <: EdgeLike[T, V]](val traveler: Traveler[T, V, E], val vertices: Iterable[V]) extends Dynamic {
  override def toString: String = {
    val properties = vertices.map(traveler.vertex(_).properties)
    JsArray(properties.toSeq).toString
  }

  def apply(property: String): Iterable[JsValue] = {
    vertices.map { vertex =>
      traveler.vertex(vertex).properties.apply(property)
    }
  }

  def applyDynamic(property: String): Iterable[JsValue] = apply(property)

  def selectDynamic(property: String): Iterable[JsValue] = apply(property)

  /** Selects all vertices of this collection which satisfy a predicate. */
  def filter(p: (Vertex) => Boolean): GremlinVertices = {
    new GremlinVertices(traveler, vertices.filter { id =>
      p(traveler.vertex(id))
    })
  }

  /** In vertices of outgoing edges. This is a shortcut to outE().inV(). */
  def in(labels: String*): GremlinVertices = {
    val neighbors = if (labels.isEmpty) {
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.from == vertex
        }.map(_.to)
      }.toSet
    } else {
      val relationships = labels.toSet
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.from == vertex && relationships.contains(edge.label)
        }.map(_.to)
      }.toSet
    }

    new GremlinVertices(traveler, neighbors)
  }

  /** Out vertices of incoming edges. This is a shortcut to inE().outV(). */
  def out(labels: String*): GremlinVertices = {
    val neighbors = if (labels.isEmpty) {
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.to == vertex
        }.map(_.from)
      }.toSet
    } else {
      val relationships = labels.toSet
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.to == vertex && relationships.contains(edge.label)
        }.map(_.from)
      }.toSet
    }

    new GremlinVertices(traveler, neighbors)
  }

  /** Incoming edges. */
  def inE(labels: String*): GremlinEdges = {
    val edges = if (labels.isEmpty) {
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.to == vertex
        }
      }
    } else {
      val relationships = labels.toSet
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.to == vertex && relationships.contains(edge.label)
        }
      }
    }

    new GremlinEdges(traveler, edges)
  }

  /** Outgoing edges. */
  def outE(labels: String*): GremlinEdges = {
    val edges = if (labels.isEmpty) {
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.from == vertex
        }
      }
    } else {
      val relationships = labels.toSet
      vertices.flatMap { vertex =>
        traveler.vertex(vertex).edges.filter { edge =>
          edge.from == vertex && relationships.contains(edge.label)
        }
      }
    }

    new GremlinEdges(traveler, edges)
  }
}

class GremlinEdges(val traveler: Traveler, val edges: Iterable[Edge]) extends Dynamic {

  override def toString = edges.mkString("[\n  ", "\n  ", "\n]")

  def apply(property: String): Iterable[JsValue] = {
    edges.map(_.properties.apply(property))
  }

  def applyDynamic(property: String): Iterable[JsValue] = apply(property)

  def selectDynamic(property: String): Iterable[JsValue] = apply(property)

  /** Selects all edges of this collection which satisfy a predicate. */
  def filter(p: (Edge) => Boolean): GremlinEdges = {
    new GremlinEdges(traveler, edges.filter(p))
  }

  /** In vertices. */
  def inV(labels: String*): GremlinVertices = {
    val vertices = if (labels.isEmpty) {
      edges.map(_.to).toSet
    } else {
      val relationships = labels.toSet
      edges.filter { edge => relationships.contains(edge.label) }.map(_.to).toSet
    }

    new GremlinVertices(traveler, vertices)
  }

  /** Out vertices. */
  def outV(labels: String*): GremlinVertices = {
    val vertices = if (labels.isEmpty) {
      edges.map(_.from).toSet
    } else {
      val relationships = labels.toSet
      edges.filter { edge => relationships.contains(edge.label) }.map(_.from).toSet
    }

    new GremlinVertices(traveler, vertices)
  }
}
  */
