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

import java.nio.ByteBuffer
import unicorn.bigtable.{Column, Row}
import unicorn.json._
import unicorn.util._

/** Graph serializer. By default, edge label size is up to 256,
  * vertex property size is up to 64KB, overall data size of each edge is up to 10MB.
  *
  * @author Haifeng Li
  */
class GraphSerializer(
  val buffer: ByteBuffer = ByteBuffer.allocate(1024),
  val vertexSerializer: ColumnarJsonSerializer = new ColumnarJsonSerializer(ByteBuffer.allocate(65536)),
  val edgeSerializer: BsonSerializer = new BsonSerializer(ByteBuffer.allocate(10485760))) extends Logging {

  /** Serializes vertex id. */
  def serialize(id: Long): ByteArray = {
    buffer.clear
    buffer.putLong(id)
    buffer
  }

  /** Serializes the document vertex lookup table row key. */
  def serialize( key: String): ByteArray = {
    buffer.clear
    vertexSerializer.serialize(buffer, key)
    buffer
  }

  /** Serializes vertex property data. */
  def serializeVertex(json: JsObject): Seq[Column] = {
    vertexSerializer.serialize(json).map { case (path, value) =>
      Column(vertexSerializer.str2Bytes(path), value)
    }.toSeq
  }

  def deserializeVertex(row: Row): Vertex = {
    val vertex = deserializeVertexId(row.key)
    val families = row.families

    val properties = families.find(_.family == GraphVertexColumnFamily).map { family =>
      deserializeVertexProperties(family.columns)
    }

    val in = families.find(_.family == GraphInEdgeColumnFamily).map { family =>
      family.columns.map { column =>
        val (label, source) = deserializeEdgeColumnQualifier(column.qualifier)
        val properties = deserializeEdgeProperties(column.value)
        Edge(source, label, vertex, properties)
      }
    }.getOrElse(Seq.empty)

    val out = families.find(_.family == GraphOutEdgeColumnFamily).map { family =>
      family.columns.map { column =>
        val (label, target) = deserializeEdgeColumnQualifier(column.qualifier)
        val properties = deserializeEdgeProperties(column.value)
        Edge(vertex, label, target, properties)
      }
    }.getOrElse(Seq.empty)

    val edges = (in.size, out.size) match {
      case (0, _) => out
      case (_, 0) => in
      case _ => out ++ in
    }

    Vertex(vertex, properties.getOrElse(JsObject("id" -> JsLong(vertex))), edges)
  }

  /** Deserializes vertex property data. */
  def deserializeVertexProperties(columns: Seq[Column]): JsObject = {
    val map = columns.map { case Column(qualifier, value, _) =>
      (new String(qualifier, vertexSerializer.charset), value.bytes)
    }.toMap
    vertexSerializer.deserialize(map).asInstanceOf[JsObject]
  }

  /** Serializes an edge column qualifier. */
  def serializeEdgeColumnQualifier(label: Array[Byte], vertex: Long): Array[Byte] = {
    buffer.clear
    buffer.put(label)
    buffer.put(0.toByte)
    buffer.putLong(vertex)
    buffer
  }

  /** Deserializes an edge column qualifier. */
  def deserializeEdgeColumnQualifier(bytes: Array[Byte]): (String, Long) = {
    val buffer = ByteBuffer.wrap(bytes)
    val label = edgeSerializer.cstring(buffer)
    val vertex = buffer.getLong
    (label, vertex)
  }

  /** Serializes edge property data. */
  def serializeEdge(json: JsValue): Array[Byte] = {
    edgeSerializer.clear
    edgeSerializer.put(json)
    edgeSerializer.toBytes
  }

  /** Deserializes vertex id. */
  def deserializeVertexId(bytes: Array[Byte]): Long = {
    require(bytes.length == 8, s"vertex id bytes size is not 8: ${bytes.length}")
    ByteBuffer.wrap(bytes).getLong
  }

  /** Deserializes edge property data. */
  def deserializeEdgeProperties(bytes: Array[Byte]): JsValue = {
    edgeSerializer.deserialize(bytes)
  }
}
