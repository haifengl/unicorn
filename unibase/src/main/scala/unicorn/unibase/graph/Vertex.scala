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
import unicorn.json._

/*
/** Graph vertex.
  *
  * @author Haifeng Li
  */
case class Vertex(val id: Long, val properties: JsObject, val edges: Seq[Edge]) extends Dynamic {

  /** In vertices of outgoing edges. */
  @transient lazy val in: Map[String, Seq[Long]] = {
    edges.filter(_.from == id).groupBy(_.label).mapValues(_.map(_.to))
  }

  /** Out vertices of incoming vertices. */
  @transient lazy val out: Map[String, Seq[Long]] = {
    edges.filter(_.to == id).groupBy(_.label).mapValues(_.map(_.from))
  }

  /** Incoming arcs. */
  @transient lazy val inE: Map[String, Seq[Edge]] = {
    edges.filter(_.to == id).groupBy(_.label)
  }

  /** Outgoing arcs. */
  @transient lazy val outE: Map[String, Seq[Edge]] = {
    edges.filter(_.from == id).groupBy(_.label)
  }

  /* Neighbor vertices. */
  @transient lazy val neighbors: Seq[Long] = {
    edges.map { case Edge(from, _, to, _) =>
        if (from == id) to else from
    }
  }

  override def toString = s"Vertex[$id] = ${properties.prettyPrint}"

  def apply(property: String): JsValue = properties.apply(property)

  def applyDynamic(property: String): JsValue = apply(property)

  def selectDynamic(property: String): JsValue = apply(property)
}
*/