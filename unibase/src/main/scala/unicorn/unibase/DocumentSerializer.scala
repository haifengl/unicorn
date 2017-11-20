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

package unicorn.unibase

import java.nio.ByteBuffer

import unicorn.bigtable.{Column, ColumnFamily}
import unicorn.json._

/** Document serializer.
  *
  * @author Haifeng Li
  */
class DocumentSerializer {
  val serializer = new ColumnarJsonSerializer(ByteBuffer.allocate(1048576))
  val pathDelimiter = serializer.pathDelimiter

  /** Serialize document data. */
  def serialize(json: JsObject): Seq[Column] = {
    serializer.serialize(json).map { case (path, value) =>
      Column(serializer.str2Bytes(path), value)
    }.toSeq
  }

  /** Assembles the document from multi-column family data. */
  def deserialize(data: Seq[ColumnFamily]): Option[JsObject] = {
    val objects = data.map { case ColumnFamily(_, columns) =>
      val map = columns.map { case Column(qualifier, value, _) =>
        (new String(qualifier, serializer.charset), value.bytes)
      }.toMap
      val json = serializer.deserialize(map)
      json.asInstanceOf[JsObject]
    }

    objects.size match {
      case 0 => None
      case 1 => Some(objects(0))
      case _ =>
        val fold = objects.foldLeft(JsObject()) { (doc, family) =>
          doc.fields ++= family.fields
          doc
        }
        Some(fold)
    }
  }
}
