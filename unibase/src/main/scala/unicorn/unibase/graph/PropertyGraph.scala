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
import org.apache.hadoop.hbase.util.{Order, OrderedBytes, SimplePositionedMutableByteRange}
import unicorn.bigtable.{BigTable, RowScan}
import unicorn.json._
import unicorn.unibase._

/** The property graph contains connected entities (the nodes)
  * which can hold any number of attributes (key-value-pairs).
  * Nodes can be tagged with labels representing their different
  * roles in your domain. In addition to contextualizing node and
  * relationship properties, labels may also serve to attach
  * metadata—​index or constraint information—​to certain nodes.
  *
  * Relationships provide directed, named semantically relevant
  * connections between two node-entities. A relationship always
  * has a direction, a type, a start node, and an end node.
  * Like nodes, relationships can have any properties.
  * In most cases, relationships have quantitative properties,
  * such as weights, costs, distances, ratings, time intervals,
  * or strengths. As relationships are stored efficiently, two
  * nodes can share any number or type of relationships without
  * sacrificing performance. Note that although they are directed,
  * relationships can always be navigated regardless of direction.
  *
  * @author Haifeng Li
  */

/** Node/entity in property graph. */
case class Node(id: ObjectId, label: String, properties: JsObject) extends VertexId[ObjectId] {
  override def key: Key = ObjectIdKey(id)
}

/** A semantic triple, or simply triple, is the atomic data entity in the
  * Resource Description Framework (RDF) data model. A triple is a set of
  * three entities that codifies a statement about semantic data in the form
  * of subject–predicate–object expressions. For example,
  * "The sky has the color blue", consist of a subject ("the sky"),
  * a predicate ("has the color"), and an object ("blue").
  * From this basic structure, triples can be composed into more complex
  * models, by using triples as objects or subjects of other triples.
  */
/** A graph edge with relationship label.
  * Besides, the relationship may have optional properties.
  *
  * @author Haifeng Li
  */
case class Relationship(override val from: Node, override val to: Node, val label: String, val properties: JsValue = JsUndefined) extends EdgeLike[ObjectId, Node] with Dynamic {

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

class PropertyGraph(val table: BigTable with RowScan) extends GraphLike[ObjectId, Node, Relationship] {
  override def key(edge: Relationship): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(1024)
    OrderedBytes.encodeString(range, edge.subject.entity, Order.ASCENDING)
    OrderedBytes.encodeString(range, edge.predicate, Order.ASCENDING)
    OrderedBytes.encodeString(range, edge.`object`.entity, Order.ASCENDING)
    range.getBytes.slice(0, range.getPosition)
  }

  override def decode(key: Array[Byte], value: Array[Byte]): Relationship = {
    val range = new SimplePositionedMutableByteRange(key)
    val subject = OrderedBytes.decodeString(range)
    val predicate = OrderedBytes.decodeString(range)
    val `object` = OrderedBytes.decodeString(range)
    Triple(subject, predicate, `object`)
  }
}
