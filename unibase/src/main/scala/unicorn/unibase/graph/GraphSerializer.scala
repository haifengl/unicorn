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

import java.nio.ByteBuffer
import unicorn.bigtable._
import unicorn.json._

/** Graph serializer. By default, edge label size is up to 256,
  * The property data size of each edge is up to 100KB.
  *
  * @author Haifeng Li
  */
class GraphSerializer(
  val buffer: ByteBuffer = ByteBuffer.allocate(1024),
  val edgeSerializer: JsonSerializer = new JsonSerializer(ByteBuffer.allocate(102400))) {

  /** Serializes an edge column qualifier.
    * @param label The relationship label.
    * @param vertexTable The table of vertex
    * @param vertexKey The vertex key.
    */
  def serializeEdgeColumnQualifier(label: String, vertexTable: String, vertexKey: Array[Byte]): Array[Byte] = {
    val NULL = 0.toByte
    buffer.clear
    buffer.put(label)
    buffer.put(NULL)
    buffer.put(vertexTable)
    buffer.put(NULL)
    buffer.put(vertexKey)
    buffer
  }

  /** Deserializes an edge column qualifier. */
  def deserializeEdgeColumnQualifier(bytes: Array[Byte]): (String, String, Array[Byte]) = {
    val NULL = 0.toByte
    var pos = 0
    while (bytes(pos) != NULL) pos = pos + 1
    val label = new String(bytes, 0, pos, UTF8)

    pos = pos + 1
    val start = pos
    while (bytes(pos) != NULL) pos = pos + 1
    val vertexTable = new String(bytes, start, pos - start, UTF8)

    val vertexKey = bytes.slice(pos+1, bytes.length)

    (label, vertexTable, vertexKey)
  }

  /** Serializes edge property data. */
  def serializeEdgeProperties(json: JsValue): Array[Byte] = {
    edgeSerializer.serialize(json)
  }

  /** Deserializes edge property data. */
  def deserializeEdgeProperties(bytes: Array[Byte]): JsValue = {
    edgeSerializer.deserialize(bytes)
  }
}
