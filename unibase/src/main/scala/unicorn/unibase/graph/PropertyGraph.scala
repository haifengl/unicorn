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
import unicorn.bigtable.OrderedBigTable
import unicorn.kv.OrderedKeyspace
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
case class Node(id: Long, properties: JsObject) extends VertexLike {
  override def key: Key = LongKey(id)
}

/** A graph edge with relationship label.
  * Besides, the relationship may have optional properties.
  *
  * @author Haifeng Li
  */
case class Relationship(override val from: LongKey, val label: String, override val to: LongKey, val properties: JsValue = JsUndefined) extends EdgeLike with Dynamic {

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

trait PropertyGraph extends GraphLike[Node, Relationship] {
  private[unibase] val serializer = new JsonSerializer

  /** Adds a node. */
  def add(node: Node): Unit

  /** Deletes a node. */
  def delete(node: Node): Unit

  private[unibase] def prefix(vertex: Key, `type`: Option[String]): Array[Byte] = {
    if (!vertex.isInstanceOf[LongKey])
      throw new IllegalArgumentException("PropertyGraph vertex key must be of Long")

    val range = new SimplePositionedMutableByteRange(128)
    OrderedBytes.encodeNumeric(range, vertex.asInstanceOf[LongKey].key, Order.ASCENDING)
    if (`type`.isDefined)
      OrderedBytes.encodeString(range, `type`.get, Order.ASCENDING)
    else
      range.put(0.toByte) // without the extra byte, the scan will start with the vertex.
    range.getBytes.slice(0, range.getPosition)
  }

  private[unibase] def key(node: Long): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(8)
    OrderedBytes.encodeNumeric(range, node, Order.ASCENDING)
    range.getBytes.slice(0, range.getPosition)
  }

  private[unibase] def key(edge: Relationship): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(128)
    OrderedBytes.encodeNumeric(range, edge.from.asInstanceOf[LongKey].key, Order.ASCENDING)
    OrderedBytes.encodeString(range, edge.label, Order.ASCENDING)
    OrderedBytes.encodeNumeric(range, edge.to.asInstanceOf[LongKey].key, Order.ASCENDING)
    range.getBytes.slice(0, range.getPosition)
  }

  private[unibase] def decode(key: Array[Byte], value: Array[Byte]): Relationship = {
    val range = new SimplePositionedMutableByteRange(key)
    val from = OrderedBytes.decodeNumericAsLong(range)
    val label = OrderedBytes.decodeString(range)
    val to = OrderedBytes.decodeNumericAsLong(range)
    val properties = serializer.deserialize(value)
    Relationship(from, label, to, properties)
  }
}

class KeyValuePropertyGraph(val table: OrderedKeyspace) extends PropertyGraph {
  override def apply(vertex: Key): Option[Node] = {
    if (!vertex.isInstanceOf[LongKey])
      throw new IllegalArgumentException("PropertyGraph vertex key must be of Long")

    val id = vertex.asInstanceOf[LongKey].key
    val bytes = table(key(id))
    bytes.map(serializer.deserialize(_)).map { json => Node(id, json.asInstanceOf[JsObject]) }
  }

  override def edges(vertex: Key, `type`: Option[String]): Iterator[Relationship] = {
    table.scan(prefix(vertex, `type`)).map { kv =>
      decode(kv.key, kv.value)
    }
  }

  override def add(node: Node): Unit = {
    table(key(node.id)) = serializer.serialize(node.properties)
  }

  override def delete(node: Node): Unit = {
    table.delete(key(node.id))
  }

  override def add(edge: Relationship): Unit = {
    table(key(edge)) = serializer.serialize(edge.properies)
  }

  override def delete(edge: Relationship): Unit = {
    table.delete(key(edge))
  }
}

class BigTablePropertyGraph(val table: OrderedBigTable) extends PropertyGraph {
  override def apply(vertex: Key): Option[Node] = {
    if (!vertex.isInstanceOf[LongKey])
      throw new IllegalArgumentException("PropertyGraph vertex key must be of Long")

    val id = vertex.asInstanceOf[LongKey].key
    val bytes = table(key(id), DocumentColumnFamily, DocumentColumn)
    bytes.map(serializer.deserialize(_)).map { json => Node(id, json.asInstanceOf[JsObject]) }
  }

  override def edges(vertex: Key, `type`: Option[String]): Iterator[Relationship] = {
    table.scanPrefix(prefix(vertex, `type`), DocumentColumnFamily).map { row =>
      val column = row.families(0).columns(0)
      decode(row.key, column.value)
    }
  }

  override def add(node: Node): Unit = {
    table(key(node.id), DocumentColumnFamily, DocumentColumn) = serializer.serialize(node.properties)
  }

  override def delete(node: Node): Unit = {
    table.delete(key(node.id), DocumentColumnFamily, DocumentColumn)
  }

  override def add(edge: Relationship): Unit = {
    table(key(edge), DocumentColumnFamily, DocumentColumn) = serializer.serialize(edge.properies)
  }

  override def delete(edge: Relationship): Unit = {
    table.delete(key(edge), DocumentColumnFamily, DocumentColumn)
  }
}