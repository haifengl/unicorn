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

import org.apache.hadoop.hbase.util.{Order, OrderedBytes, SimplePositionedMutableByteRange}
import unicorn.bigtable.OrderedBigTable
import unicorn.json.JsonSerializer
import unicorn.kv.OrderedKeyspace
import unicorn.unibase._

/** A knowledge graph or semantic network, is a graph that represents semantic
  * relations between concepts. This is often used as a form of knowledge
  * representation. It is a directed graph consisting of vertices, which
  * represent concepts, and edges, which represent semantic relations
  * between concepts.
  *
  * The relations are in the form of subject–predicate–object expressions.
  * These expressions are known as triples in RDF terminology.
  * The subject denotes the entity, and the predicate denotes
  * traits or aspects of the entity and expresses a relationship
  * between the subject and the object. For example, one way to represent
  * the notion "The sky has the color blue" in RDF is as the triple:
  * a subject denoting "the sky", a predicate denoting "has the color",
  * and an object denoting "blue".
  *
  * @author Haifeng Li
  */

/** Entity vertex in knowledge/semantic graph. */
case class Entity(id: String) extends VertexLike[String] {
  override def key: Key = StringKey(id)
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
case class Triple(from: Entity, predicate: String, to: Entity) extends EdgeLike[String, Entity] {
  def subject = from
  def `object` = to
}

object Triple {
  def apply(subject: String, predicate: String, `object`: String): Triple = {
    Triple(Entity(subject), predicate, Entity(`object`))
  }
}

trait SemanticGraph extends GraphLike[String, Entity, Entity, Triple] {
  override def apply(vertex: String): Option[Entity] = {
    if (edges(vertex).isEmpty) None else Some(Entity(vertex))
  }

  override def apply(vertex: Entity): Option[Entity] = {
    if (edges(vertex).isEmpty) None else Some(vertex)
  }

  override def edges(vertex: String): Iterator[Triple] = {
    edges(StringKey(vertex))
  }

  override def edges(vertex: Entity): Iterator[Triple] = {
    edges(vertex.key)
  }

  override def edges(vertex: String, `type`: String): Iterator[Triple] = {
    val prefix = CompositeKey(StringKey(vertex), StringKey(`type`))
    edges(prefix)
  }

  override def edges(vertex: Entity, `type`: String): Iterator[Triple] = {
    val prefix = CompositeKey(StringKey(vertex.id), StringKey(`type`))
    edges(prefix)
  }

  private[unibase] def edges(prefix: Key): Iterator[Triple]

  override def add(edge: Triple): Unit

  override def delete(edge: Triple): Unit

  private[unibase] def key(edge: Triple): Array[Byte] = {
    val range = new SimplePositionedMutableByteRange(1024)
    OrderedBytes.encodeString(range, edge.subject.id, Order.ASCENDING)
    OrderedBytes.encodeString(range, edge.predicate, Order.ASCENDING)
    OrderedBytes.encodeString(range, edge.`object`.id, Order.ASCENDING)
    range.getBytes.slice(0, range.getPosition)
  }

  private[unibase] def decode(key: Array[Byte], value: Array[Byte]): Triple = {
    val range = new SimplePositionedMutableByteRange(key)
    val subject = OrderedBytes.decodeString(range)
    val predicate = OrderedBytes.decodeString(range)
    val `object` = OrderedBytes.decodeString(range)
    Triple(subject, predicate, `object`)
  }
}

class KeyValueSemanticGraph(val table: OrderedKeyspace) extends SemanticGraph {
  private[unibase] def edges(prefix: Key): Iterator[Triple] = {
    table.scan(RowKey(prefix)).map { kv =>
      decode(kv.key, kv.value)
    }
  }

  override def add(edge: Triple): Unit = {
    table(key(edge)) = JsonSerializer.undefined
  }

  override def delete(edge: Triple): Unit = {
    table.delete(key(edge))
  }
}

class BigTableSemanticGraph(val table: OrderedBigTable) extends SemanticGraph {
  private[unibase] def edges(prefix: Key): Iterator[Triple] = {
    table.scanPrefix(RowKey(prefix), DocumentColumnFamily).map { row =>
      val column = row.families(0).columns(0)
      decode(row.key, column.value)
    }
  }

  override def add(edge: Triple): Unit = {
    table(key(edge), DocumentColumnFamily, DocumentColumn) = JsonSerializer.undefined
  }

  override def delete(edge: Triple): Unit = {
    table.delete(key(edge), DocumentColumnFamily, DocumentColumn)
  }
}
