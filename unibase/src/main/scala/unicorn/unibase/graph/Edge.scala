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
import unicorn.json.{JsUndefined, JsValue}

/** Graph edge template.
  *
  * @tparam V the user type of the vertices of this edge.
  *
  * @author Haifeng Li
  */
trait EdgeLike[V <: VertexLike] {
  /** The end point of this edge. */
  val from: VertexLike
  /** The end point of this edge. */
  val to: VertexLike
}

case class Edge[V <: VertexLike](val from: V, val to: V) extends EdgeLike[V]

/** A graph edge with relationship label.
  * Besides, the relationship may have optional properties.
  *
  * @author Haifeng Li
  */
case class Relationship[V <: VertexLike](override val from: V, override val to: V, val label: String, val properties: JsValue = JsUndefined) extends EdgeLike[V] with Dynamic {

  override def toString = {
    if (properties != JsUndefined)
      s"($from - [$label] -> $to) = ${properties.prettyPrint}"
    else
      s"($from - [$label] -> $to)"
  }

  def apply(property: String): JsValue = {
    properties.apply(property)
  }

  def applyDynamic(property: String): JsValue = apply(property)

  def selectDynamic(property: String): JsValue = apply(property)
}