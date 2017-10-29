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

package unicorn.unibase

import unicorn.json._
import unicorn.bigtable._

/** Tables are the data containers of documents. A document is simply a JSON
  * object with a unique id (the _id field), which is similar to the primary key
  * in relational database. The key can be arbitrary JSON
  * value including Object and Array. However, they are some limitations in practice:
  *
  *  - The size limitation. The id of document serves as the row key in the underlying
  *    BigTable implementation. Most BigTable implementations have the upper limit of
  *    row key (often as 64KB).
  *  - For compound primary key (i.e. multiple fields), it is better use JsArray instead
  *    of JsObject as the container because it maintains the order of fields, which is
  *    important in the scan operations. When serializing an Object, the order of fields
  *    may be undefined (because of hashing) or simply ascending in field name.
  *  - The fields of compound primary key should be fixed size. In database with schema
  *    (e.g. relational database). The variable length field is padded to the maximum size.
  *    However, Unibase is schemaless database and thus we are lack of this type information.
  *    It is the developer's responsibility to make sure the proper use of primary key.
  *    Otherwise, the index and scan operations won't work correctly.
  *
  * @author Haifeng Li
  */
class Table(override val table: BigTable with RowScan, override val meta: JsObject) extends Drawer(table, meta) {
  def scan: DocumentScanner = {
    new DocumentScanner(table.scanAll())
  }
}

/** Row scan iterator */
class DocumentScanner(rows: RowScanner) extends Iterator[JsObject] {
  val serializer = new DocumentSerializer()

  def close: Unit = rows.close

  override def hasNext: Boolean = rows.hasNext

  override def next: JsObject = {
    serializer.deserialize(rows.next.families).get
  }
}