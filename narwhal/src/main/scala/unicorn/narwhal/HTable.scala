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

package unicorn.narwhal

import java.util.{UUID, Date}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import unicorn.json._
import unicorn.bigtable._, hbase.HBaseTable
//import unicorn.index._, IndexType._, IndexSortOrder._
import unicorn.oid.BsonObjectId
import unicorn.unibase._
import unicorn.util._

/** Unibase table specialized for HBase with additional functions such
  * as \$inc, \$rollback, find, etc. */
class HTable(override val table: HBaseTable, meta: JsObject) extends Table(table, meta) {
  import unicorn.unibase.{$id, $tenant}

  /** Visibility expression which can be associated with a cell.
    * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
    */
  def setCellVisibility(expression: String): Unit = table.setCellVisibility(expression)

  /** Returns the current visibility expression setting. */
  def getCellVisibility: String = table.getCellVisibility

  /** Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access. */
  def setAuthorizations(labels: String*): Unit = table.setAuthorizations(labels: _*)

  /** Returns the current authorization labels. */
  def getAuthorizations: Seq[String] = table.getAuthorizations

  /** Gets a document. */
  def apply(asOfDate: Date, id: Int, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: Long, fields: String*): Option[JsObject] = {
    apply(JsLong(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: String, fields: String*): Option[JsObject] = {
    apply(JsString(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: Date, fields: String*): Option[JsObject] = {
    apply(JsTimestamp(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: UUID, fields: String*): Option[JsObject] = {
    apply(JsUUID(id))
  }

  /** Gets a document. */
  def apply(asOfDate: Date, id: BsonObjectId, fields: String*): Option[JsObject] = {
    apply(JsObjectId(id))
  }

  /** Gets a document.
    *
    * @param id document id.
    * @param fields top level fields to retrieve.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(asOfDate: Date, id: JsValue, fields: String*): Option[JsObject] = {
    if (fields.isEmpty) {
      val data = table.getAsOf(asOfDate, key(id), families)
      serializer.deserialize(data)
    } else {
      val projection = JsObject(fields.map(_ -> JsInt(1)): _*)
      projection($id) = id
      get(asOfDate, projection)
    }
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
    * @param asOfDate snapshot of document is at this point of time
    * @param projection an object that specifies the fields to return. The _id field should be included to
    *                   indicate which document to retrieve.
    * @return the projected document. The _id field will be always included.
    */
  def get(asOfDate: Date, projection: JsObject): Option[JsObject] = {
    val id = _id(projection)
    val families = project(projection)
    val data = table.getAsOf(asOfDate, key(id), families)
    val doc = serializer.deserialize(data)
    doc.map(_($id) = id)
    doc
  }

  /** Updates a document. The supported update operators include
    *
    * - \$set: Sets the value of a field in a document.
    * - \$unset: Removes the specified field from a document.
    * - \$inc: Increments the value of the field by the specified amount.
    * - \$rollback: Rolls back to previous version.
    *
    * @param doc the document update operators.
    */
  override def update(doc: JsObject): Unit = {
    val $inc = doc("$inc")
    require($inc == JsUndefined || $inc.isInstanceOf[JsObject], "$inc is not an object: " + $inc)

    val $rollback = doc("$rollback")
    require($rollback == JsUndefined || $rollback.isInstanceOf[JsObject], "$rollback is not an object: " + $rollback)

    super.update(doc)

    val id = _id(doc)

    if ($inc.isInstanceOf[JsObject]) inc(id, $inc.asInstanceOf[JsObject])

    if ($rollback.isInstanceOf[JsObject]) rollback(id, $rollback.asInstanceOf[JsObject])
  }

  /** The \$inc operator accepts positive and negative values.
    *
    * The field must exist. Increase a nonexist counter will create
    * the column in BigTable. However, it won't show up in the parent
    * object. We could check and add it to the parent. However it is
    * expensive and we lose many benefits of built-in counters (efficiency
    * and atomic). It is the user's responsibility to create the counter
    * first.
    *
    * @param id the id of document.
    * @param doc the fields to increase/decrease.
    */
  def inc(id: JsValue, doc: JsObject): Unit = {
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: inc ${$id}")
    require(!doc.fields.exists(_._1 == $tenant), s"Invalid operation: inc ${$tenant}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) => familyOf(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, JsLong(value)) => (ByteArray(valueSerializer.str2PathBytes(field)), value)
        case (field, JsInt(value)) => (ByteArray(valueSerializer.str2PathBytes(field)), value.toLong)
        case (_, value) => throw new IllegalArgumentException(s"Invalid value: $value")
      }
      (family, columns)
    }

    table.addCounter(key(id), families)
  }

  /** The \$rollover operator roll particular fields back to previous version.
    *
    * The document key _id should not be rollover.
    *
    * Note that if rollover, the latest version will be lost after a major compaction.
    *
    * @param id the id of document.
    * @param doc the fields to delete.
    */
  def rollback(id: JsValue, doc: JsObject): Unit = {
    require(!doc.fields.exists(_._1 == $id), s"Invalid operation: rollover ${$id}")
    require(!doc.fields.exists(_._1 == $tenant), s"Invalid operation: rollover ${$tenant}")

    val groups = doc.fields.toSeq.groupBy { case (field, _) => familyOf(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val columns = fields.map {
        case (field, _) => ByteArray(valueSerializer.str2PathBytes(field))
      }
      (family, columns)
    }

    table.rollback(key(id), families)
  }

  /** Use checkAndPut for insert. */
  override def insert(doc: JsObject): Unit = {
    val id = _id(doc)
    val groups = doc.fields.toSeq.groupBy { case (field, _) => locality(field) }

    val families = groups.toSeq.map { case (family, fields) =>
      val json = JsObject(fields: _*)
      val columns = serializer.serialize(json)
      ColumnFamily(family, columns)
    }

    val checkFamily = locality($id)
    require(table.checkAndPut(key(id), checkFamily, idColumnQualifier, families), s"Document $id already exists")
  }

  /** Returns one document that satisfies the specified query criteria.
    * If multiple documents satisfy the query, this method returns the
    * first document according to the document key order.
    * If no document satisfies the query, the method returns None.
    */
  def findOne(query: JsObject = JsObject(), projection: JsObject = JsObject()): Option[JsObject] = {
    val it = find(query, projection)
    if (it.hasNext) Some(it.next) else None
  }

  /** Searches the table.
    * @param projection an object that specifies the fields to return. Empty projection object returns the whole document.
    * @param query the query predict object in MongoDB style. Supported operators include \$and, \$or, \$eq, \$ne,
    *              \$gt, \$gte (or \$ge), \$lt, \$lte (or \$le), and \$exists.
    *              When the test value is true, \$exists matches the documents that contain the field,
    *              including documents where the field value is null. If the test value is false, the
    *              query returns only the documents that do not contain the field.
    * @return an iterator of matched document.
    */
  def find(query: JsObject = JsObject(), projection: JsObject = JsObject()): Iterator[JsObject] = {
    val families = if (projection.fields.isEmpty) Seq.empty else project(projection)

    val it = if (query.fields.isEmpty) {
      tenant match {
        case JsUndefined => table.scanAll(families)
        case _ => table.scanPrefix(serializer.tenantRowKeyPrefix(tenant), families)
      }
    } else {
      val filter = scanFilter(query)
      tenant match {
        case JsUndefined => table.filterScanAll(filter, families)
        case _ => table.filterScanPrefix(filter, serializer.tenantRowKeyPrefix(tenant), families)
      }
    }

    it.map { data =>
      serializer.deserialize(data.families).get
    }
  }

  /** Returns one document that satisfies the specified query criteria.
    * If multiple documents satisfy the query, this method returns the
    * first document according to the document key order.
    * If no document satisfies the query, the method returns None.
    */
  def findOne(where: String, fields: String*): Option[JsObject] = {
    val it = find(where, fields: _*)
    if (it.hasNext) Some(it.next) else None
  }

  /** Searches the table with SQL like where clause.
    * @param where SQL where clause like expression.
    * @param fields The list of fields to return.
    * @return an iterator of matched document.
    */
  def find(where: String, fields: String*): Iterator[JsObject] = {

    val query = FilterExpression(where)
    val families = if (fields.isEmpty) Seq.empty else project(fields)

    val filter = scanFilter(query)
    val it = tenant match {
      case JsUndefined => table.filterScanAll(filter, families)
      case _ => table.filterScanPrefix(filter, serializer.tenantRowKeyPrefix(tenant), families)
    }

    it.map { data =>
      serializer.deserialize(data.families).get
    }
  }

  /** Returns the number of documents matching the search criteria.
    * The `query` parameter should not be empty. Otherwise, it scan the whole
    * table, which is very slow. We remove the default value for this
    * parameter to discourage pass an empty query object.
    *
    * In a multi-tenancy environment, it may not be too slow to count
    * all documents for a tenant if it is not big.
    *
    * @param query the query predict object in MongoDB style. Supported operators include \$and, \$or, \$eq, \$ne,
    *              \$gt, \$gte (or \$ge), \$lt, \$lte (or \$le), and \$exists.
    *              When the test value is true, \$exists matches the documents that contain the field,
    *              including documents where the field value is null. If the test value is false, the
    *              query returns only the documents that do not contain the field.
    */
  def count(query: JsObject): Int = {
    val it = if (query.fields.isEmpty) {
      tenant match {
        case JsUndefined => table.scanAll(familyOf($id), idColumnQualifier)
        case _ => table.scanPrefix(serializer.tenantRowKeyPrefix(tenant), familyOf($id), idColumnQualifier)
      }
    } else {
      val filter = scanFilter(query)
      tenant match {
        case JsUndefined => table.filterScanAll(filter, familyOf($id), idColumnQualifier)
        case _ => table.filterScanPrefix(filter, serializer.tenantRowKeyPrefix(tenant), familyOf($id), idColumnQualifier)
      }
    }

    it.size
  }

  /** Returns the number of documents matching the search criteria.
    * @param where SQL where clause like expression.
    */
  def count(where: String): Int = {

    val query = FilterExpression(where)

    val filter = scanFilter(query)
    val it = tenant match {
      case JsUndefined => table.filterScanAll(filter, familyOf($id), idColumnQualifier)
      case _ => table.filterScanPrefix(filter, serializer.tenantRowKeyPrefix(tenant), familyOf($id), idColumnQualifier)
    }

    it.size
  }

  /** Returns the scan filter based on the query predicts.
    *
    * @param query query predict object.
    * @return scan filter.
    */
  private def scanFilter(query: JsObject): ScanFilter.Expression = {

    val filters = query.fields.map {
      case ("$or", condition) =>
        require(condition.isInstanceOf[JsArray], "$or predict is not an array")

        val filters = condition.asInstanceOf[JsArray].elements.map { e =>
          require(e.isInstanceOf[JsObject], s"or predict element $e is not an object")
          scanFilter(e.asInstanceOf[JsObject])
        }

        require(!filters.isEmpty, "find: empty $or array")

        if (filters.size > 1) ScanFilter.Or(filters) else filters(0)

      case ("$and", condition) =>
        require(condition.isInstanceOf[JsArray], "$and predict is not an array")

        val filters = condition.asInstanceOf[JsArray].elements.map { e =>
          require(e.isInstanceOf[JsObject], s"and predict element $e is not an object")
          scanFilter(e.asInstanceOf[JsObject])
        }

        require(!filters.isEmpty, "find: empty $and array")

        if (filters.size > 1) ScanFilter.And(filters) else filters(0)

      case (field, condition) => condition match {
        case JsObject(fields) => fields.toSeq match {
          case Seq(("$eq", value)) => basicFilter(ScanFilter.CompareOperator.Equal, field, value)
          case Seq(("$ne", value)) => basicFilter(ScanFilter.CompareOperator.NotEqual, field, value)
          case Seq(("$gt", value)) => basicFilter(ScanFilter.CompareOperator.Greater, field, value)
          case Seq(("$ge", value)) => basicFilter(ScanFilter.CompareOperator.GreaterOrEqual, field, value)
          case Seq(("$gte", value)) => basicFilter(ScanFilter.CompareOperator.GreaterOrEqual, field, value)
          case Seq(("$lt", value)) => basicFilter(ScanFilter.CompareOperator.Less, field, value)
          case Seq(("$le", value)) => basicFilter(ScanFilter.CompareOperator.LessOrEqual, field, value)
          case Seq(("$lte", value)) => basicFilter(ScanFilter.CompareOperator.LessOrEqual, field, value)
          case Seq(("$exists", value)) =>
            value match {
              case JsTrue  => basicFilter(ScanFilter.CompareOperator.NotEqual, field, JsUndefined)
              case JsFalse => basicFilter(ScanFilter.CompareOperator.Equal, field, JsUndefined, false)
              case _ => throw new IllegalArgumentException(s"Invalid $$exists operator value: $value. Only boolean value accepted.")
            }
        }
        case _ => basicFilter(ScanFilter.CompareOperator.Equal, field, condition)
      }
    }.toSeq

    require(!filters.isEmpty, "find: empty filter object")

    if (filters.size > 1) ScanFilter.And(filters) else filters(0)
  }

  private def basicFilter(op: ScanFilter.CompareOperator.Value, field: String, value: JsValue, filterIfMissing: Boolean = true): ScanFilter.Expression = {
    val bytes = value match {
      case x: JsBoolean => serializer.valueSerializer.serialize(x)
      case x: JsInt => serializer.valueSerializer.serialize(x)
      case x: JsLong => serializer.valueSerializer.serialize(x)
      case x: JsDouble => serializer.valueSerializer.serialize(x)
      case x: JsString => serializer.valueSerializer.serialize(x)
      case x: JsDate => serializer.valueSerializer.serialize(x)
      case x: JsUUID => serializer.valueSerializer.serialize(x)
      case x: JsObjectId => serializer.valueSerializer.serialize(x)
      case x: JsBinary => serializer.valueSerializer.serialize(x)
      case JsUndefined => serializer.valueSerializer.undefined
      case _ => throw new IllegalArgumentException(s"Unsupported predict: $field $op $value")
    }

    ScanFilter.BasicExpression(op, familyOf(field), ByteArray(valueSerializer.str2PathBytes(field)), bytes, filterIfMissing)
  }

  private def project(fields: Seq[String]): Seq[(String, Seq[ByteArray])] = {

    val groups = fields.groupBy { field =>
      familyOf(field)
    }

    // We need to get the whole column family.
    // If the path is to a nested object, we will miss the children if not read the
    // whole column family. If BigTable implementations support reading columns
    // by prefix, we can do it more efficiently.
    groups.toSeq.map { case (family, _) => (family, Seq.empty) }
  }


  /** Converts a FilterExpression to ScanFilter.Expression. */
  def scanFilter(filter: FilterExpression): ScanFilter.Expression = filter match {

    case And(left, right) => ScanFilter.And(Seq(scanFilter(left), scanFilter(right)))
    case Or(left, right) => ScanFilter.Or(Seq(scanFilter(left), scanFilter(right)))
    case Eq(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.Equal, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case Ne(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.NotEqual, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case Gt(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.Greater, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case Ge(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.GreaterOrEqual, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case Lt(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.Less, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case Le(left, right) => ScanFilter.BasicExpression(ScanFilter.CompareOperator.LessOrEqual, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), serialize(right))
    case IsNull(left, right) =>
      val op = if (right) ScanFilter.CompareOperator.NotEqual else ScanFilter.CompareOperator.Equal
      ScanFilter.BasicExpression(op, familyOf(left), ByteArray(valueSerializer.str2PathBytes(left)), valueSerializer.undefined, right)
  }

  private def serialize(value: Literal): Array[Byte] = value match {
    case StringLiteral(x) => serializer.valueSerializer.serialize(JsString(x))
    case IntLiteral(x) => serializer.valueSerializer.serialize(JsInt(x))
    case DoubleLiteral(x) => serializer.valueSerializer.serialize(JsDouble(x))
    case DateLiteral(x) => serializer.valueSerializer.serialize(JsTimestamp(x))
  }

  /** Writes the given scan into a Base64 encoded string.
    *
    * @param scan  The scan to write out.
    * @return The scan saved in a Base64 encoded string.
    */
  private def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
  }

  /** Returns a Spark RDD of query.
    *
    * @param sc Spark context object.
    * @param projection an object that specifies the fields to return. Empty projection object returns the whole document.
    * @param query the query predict object in MongoDB style. Supported operators include \$and, \$or, \$eq, \$ne,
    *              \$gt, \$gte (or \$ge), \$lt, \$lte (or \$le), and \$exists.
    *              When the test value is true, \$exists matches the documents that contain the field,
    *              including documents where the field value is null. If the test value is false, the
    *              query returns only the documents that do not contain the field.

    * @return an RDD encapsulating the query.
    */
  def rdd(sc: SparkContext, query: JsObject = JsObject(), projection: JsObject = JsObject()): RDD[JsObject] = {
    val families = if (projection.fields.isEmpty) Seq.empty else project(projection)
    val filter = if (query.fields.isEmpty) None else Some(scanFilter(query))
    val (startRow, stopRow) = if (tenant == JsUndefined) {
      (table.startRowKey, table.endRowKey)
    } else {
      val prefix = serializer.tenantRowKeyPrefix(tenant)
      (ByteArray(prefix), ByteArray(table.nextRowKeyForPrefix(prefix)))
    }

    val scan = table.hbaseScan(startRow, stopRow, families, filter)
    scan.setCaching(500)
    scan.setCacheBlocks(false)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, name)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val rdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    rdd.mapPartitions { it =>
      val serializer = new DocumentSerializer()
      it.map { tuple =>
        val result = tuple._2
        val row = HBaseTable.getRow(result)
        serializer.deserialize(row.families).get
      }
    }
  }

  /** Inserts a RDD of objects. */
  def insert(rdd: RDD[JsObject]): Unit = {
    rdd.mapPartitions { it =>
      val hbase = new HTable(table, meta)
      it.map(hbase.insert(_))
    }
  }

  /** Inserts a RDD of objects. */
  def upsert(rdd: RDD[JsObject]): Unit = {
    rdd.mapPartitions { it =>
      val hbase = new HTable(table, meta)
      it.map(hbase.upsert(_))
    }
  }
}
/*
class HTableWithIndex(table: HBaseTable with Indexing, meta: JsObject) extends HTable(table, meta) {
  override def tenant_=(x: JsValue) {
    super.tenant_=(x)

    _tenant match {
      case None => table.tenant = None
      case Some(tenant) => table.tenant = Some(keySerializer.toBytes(tenant))
    }
  }

  /** Creates an index. */
  def createIndex(indexName: String, fields: String*): Unit = {
    createIndex(indexName, fields.map((_, Ascending)), IndexType.Default)
  }

  /** Creates an index. */
  def createIndex(indexName: String, fields: Seq[(String, IndexSortOrder)], indexType: IndexType = IndexType.Default): Unit = {
    val (families, columns) = fields.map { case (field, order) =>
      val family = getFamily(field)
      val column = IndexColumn(jsonPath(field), order)
      (family, column)
    }.unzip

    val family = families.distinct
    require(family.size == 1, "Index fields must be in the same column family")

    table.createIndex(indexName, family(0), columns, indexType)
  }

  /** Drops an index. */
  def dropIndex(indexName: String): Unit = {
    table.dropIndex(indexName)
  }
}
*/