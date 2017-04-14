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

import java.util.{Date, UUID}
import unicorn.json._
import unicorn.bigtable._
import unicorn.oid.BsonObjectId
import unicorn.util.ByteArray

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
class Table(val table: BigTable, meta: JsObject) extends UpdateOps {
  /** Document serializer. */
  val serializer = new DocumentSerializer()

  /** For UpdateOps. */
  override val valueSerializer = serializer.valueSerializer

  /** The column qualifier of \$id field. */
  val idColumnQualifier = valueSerializer.str2PathBytes($id)

  /** Column families storing document fields. There may be other column families in the table
    * for meta data or index.
    */
  val families: Seq[(String, Seq[ByteArray])] = meta.families.asInstanceOf[JsArray].elements.map { e =>
    (e.toString, Seq.empty)
  }

  /** The table name. */
  val name = table.name

  /** A map of document fields to column families for storing of sets of fields in column families
    * separately to allow clients to scan over fields that are frequently used together efficient
    * and to avoid scanning over column families that are not requested.
    */
  private[unicorn] val locality: Map[String, String] = {
    val default = meta.apply(DefaultLocalityField).toString
    val map = meta.locality.asInstanceOf[JsObject].fields.mapValues(_.toString).toMap
    map.withDefaultValue(default)
  }

  override def key(id: JsValue): Array[Byte] = {
    serializer.serialize(tenant, id)
  }

  /** Returns the column family of a field. */
  override def familyOf(field: String): String = {
    val head = field.indexOf(valueSerializer.pathDelimiter) match {
      case -1 => field
      case end => field.substring(0, end)
    }
    locality(head)
  }

  /** True if the table is append only. */
  val appendOnly: Boolean = meta.appendOnly

  /** Optional tenant id in case of multi-tenancy. */
  var tenant: JsValue = JsUndefined

  /** Gets a document. */
  def apply(id: Int, fields: String*): Option[JsObject] = {
    apply(JsInt(id))
  }

  /** Gets a document. */
  def apply(id: Long, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(id: String, fields: String*): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Gets a document. */
  def apply(id: Date, fields: String*): Option[JsObject] = {
    apply(JsDate(id))
  }

  /** Gets a document. */
  def apply(id: UUID, fields: String*): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Gets a document. */
  def apply(id: BsonObjectId, fields: String*): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Gets a document.
    *
    * @param id document id.
    * @param fields top level fields to retrieve.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(id: JsValue, fields: String*): Option[JsObject] = {
    require(!id.isInstanceOf[JsCounter], "Document ID cannot be a counter")

    if (fields.isEmpty) {
      val data = table.get(key(id), families)
      serializer.deserialize(data)
    } else {
      val projection = JsObject(fields.map(_ -> JsInt(1)): _*)
      projection($id) = id
      get(projection)
    }
  }

  /** Returns the _id of a document. Throws exception if missing. */
  private[unicorn] def _id(doc: JsObject): JsValue = {
    val id = doc($id)

    require(id != JsNull && id != JsUndefined, s"missing ${$id}")
    require(!id.isInstanceOf[JsCounter], "${$id} cannot be JsCounter")

    id
  }

  /** Retruns true if the document exists. */
  def contains(id: JsValue): Boolean = {
    val checkFamily = locality($id)
    val $key = key(id)
    table.apply($key, checkFamily, idColumnQualifier).isDefined
  }

  /** A query may include a projection that specifies the fields of the document to return.
    * The projection limits the disk access and the network data transmission.
    * Note that the semantics is different from MongoDB due to the design of BigTable. For example, if a specified
    * field is a nested object, there is no easy way to read only the specified object in BigTable.
    * Intra-row scan may help but not all BigTable implementations support it. And if there are multiple
    * nested objects in request, we have to send multiple Get requests, which is not efficient. Instead,
    * we return the whole object of a column family if some of its fields are in request. This is usually
    * good enough for hot-cold data scenario. For instance of a table of events, each event has a
    * header in a column family and event body in another column family. In many reads, we only need to
    * access the header (the hot data). When only user is interested in the event details, we go to read
    * the event body (the cold data). Such a design is simple and efficient. Another difference from MongoDB is
    * that we don't support the excluded fields.
    *
    * @param projection an object that specifies the fields to return. The _id field should be included to
    *                   indicate which document to retrieve.
    * @return the projected document. The _id field will be always included.
    */
  def get(projection: JsObject): Option[JsObject] = {
    val id = _id(projection)
    val families = project(projection)

    val data = table.get(key(id), families)
    val doc = serializer.deserialize(data)
    doc
  }

  /** Maps a projection to the seq of column families to fetch. */
  private[unicorn] def project(projection: JsObject): Seq[(String, Seq[ByteArray])] = {

    val groups = projection.fields.map(_._1).groupBy { field =>
      familyOf(field)
    }

    // We need to get the whole column family.
    // If the path is to a nested object, we will miss the children if not read the
    // whole column family. If BigTable implementations support reading columns
    // by prefix, we can do it more efficiently.
    groups.toSeq.map { case (family, _) => (family, Seq.empty) }
  }

  /** Upserts a document. If a document with same key exists, it will overwritten.
    * The _id field of document will be used as the primary key in the table.
    * If the document doesn't have _id field, a random UUID will be generated as _id.
    *
    * @param doc the document.
    * @return the document id.
    */
  def upsert(doc: JsObject): JsValue = {
    val id = doc($id) match {
      case JsUndefined | JsNull => doc($id) = UUID.randomUUID
      case id => id
    }

    if (tenant != JsUndefined) {
      doc($tenant) = tenant
    }

    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = serializer.serialize(json)
      ColumnFamily(family, columns)
    }

    table.put(key(id), families)
    id
  }

  /** Upserts an array of documents. The elements of JsArray must be JsObject.
    *
    * @param docs an array of documents.
    * @return the list of document ids.
    */
  def upsert(docs: JsArray): Seq[JsValue] = {
    docs.map { doc =>
      upsert(doc.asInstanceOf[JsObject])
    }.toSeq
  }

  /** Inserts a document. Different from upsert, this operation checks if the document already
    * exists first.
    *
    * @param doc the document.
    * @return true if the document is inserted, false if the document already existed.
    */
  def insert(doc: JsObject): Unit = {
    val id = _id(doc)
    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val checkFamily = locality($id)
    val $key = key(id)
    require(table.apply($key, checkFamily, idColumnQualifier).isEmpty, s"Document $id already exists")

    if (tenant != JsUndefined) {
      doc($tenant) = tenant
    }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = serializer.serialize(json)
      ColumnFamily(family, columns)
    }

    table.put($key, families)
  }

  /** Inserts an array of documents. The elements of JsArray must be JsObject.
    * Note that this is not ACID. If an exception thrown during insertion of one
    * document, the elements front of it were already inserted and won't be rollback.
    *
    * @param docs an array of documents.
    */
  def insert(docs: JsArray): Unit = {
    docs.foreach { doc =>
      insert(doc.asInstanceOf[JsObject])
    }
  }

  /** Removes a document.
    *
    * @param id the document id.
    */
  def delete(id: JsValue): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException
    else
      table.delete(key(id))
  }

  /** Updates a document. The supported update operators include
    *
    *  - \$set: Sets the value of a field in a document.
    *  - \$unset: Removes the specified field from a document.
    *
    * @param doc the document update operators.
    */
  def update(doc: JsObject): Unit = {
    if (appendOnly)
      throw new UnsupportedOperationException

    val id = _id(doc)

    val $set = doc("$set")
    require($set == JsUndefined || $set.isInstanceOf[JsObject], "$set is not an object: " + $set)

    val $unset = doc("$unset")
    require($unset == JsUndefined || $unset.isInstanceOf[JsObject], "$unset is not an object: " + $unset)

    if ($set.isInstanceOf[JsObject]) set(id, $set.asInstanceOf[JsObject])

    if ($unset.isInstanceOf[JsObject]) unset(id, $unset.asInstanceOf[JsObject])
  }
}
