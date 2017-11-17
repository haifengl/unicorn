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

package unicorn.bigtable.cassandra

import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.cassandra.thrift.{Column => CassandraColumn}
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ColumnPath
import org.apache.cassandra.thrift.CounterColumn
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.Mutation
import org.apache.cassandra.thrift.NotFoundException
import org.apache.cassandra.thrift.Deletion
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import unicorn.bigtable._
import unicorn.util._

/** Cassandra keyspace adapter. Cassandra's keyspaces may be regarded as tables
  * in other NoSQL solutions such as Accumulo and HBase.
  *
  * @author Haifeng Li
  */
class CassandraTable(val db: Cassandra, val name: String, consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) extends BigTable with IntraRowScan with Counter {
  val client = db.client
  client.set_keyspace(name)
  override val columnFamilies = client.describe_keyspace(name).getCf_defs.asScala.map(_.getName)

  override def close: Unit = () // Client has no close method

  private val emptyBytes = Array[Byte]()

  override def apply(row: ByteArray, family: String, column: ByteArray): Option[ByteArray] = {
    val key = ByteBuffer.wrap(row)
    val path = new ColumnPath(family).setColumn(column)

    try {
      val result = client.get(key, path, consistency)
      Some(result.getColumn.getValue)
    } catch {
      case _: NotFoundException => None
    }
  }

  override def get(row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    if (columns.isEmpty) {
      val columns = new ArrayBuffer[Column]
      var iterator = get(row, family, emptyBytes, emptyBytes, 100).iterator
      while (iterator.hasNext) {
        columns.appendAll(iterator)
        iterator = get(row, family, columns.last.qualifier, emptyBytes, 100).iterator
        iterator.next // get ride of the first that is the last one of previous bulk
      }
      columns
    } else {
      val key = ByteBuffer.wrap(row)
      val parent = new ColumnParent(family)
      val predicate = new SlicePredicate
      columns.foreach { column =>
        predicate.addToColumn_names(ByteBuffer.wrap(column))
      }

      val slice = client.get_slice(key, parent, predicate, consistency)
      getColumns(slice)
    }
  }

  override def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily] = {
    if (families.isEmpty)
      columnFamilies.map { family => ColumnFamily(family, get(row, family)) }.filter(!_.columns.isEmpty)
    else
      families.map { case (family, columns) => ColumnFamily(family, get(row, family, columns: _*)) }.filter(!_.columns.isEmpty)
  }

  /** Get a slice of rows.
    * The default count should be sufficient for most documents.
    */
  def get(row: Array[Byte], family: String, startColumn: Array[Byte], stopColumn: Array[Byte], count: Int): Seq[Column] = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val predicate = new SlicePredicate
    val range = new SliceRange
    range.start = ByteBuffer.wrap(startColumn)
    range.finish = ByteBuffer.wrap(stopColumn)
    range.reversed = false
    range.count = count
    predicate.setSlice_range(range)

    val slice = client.get_slice(key, parent, predicate, consistency)
    getColumns(slice)
  }

  override def getBatch(rows: Seq[ByteArray], families: Seq[(String, Seq[ByteArray])]): Seq[Row] = {
    rows.map { row =>
      val result = get(row, families)
      Row(row, result)
    }.filter(!_.families.isEmpty)
  }

  override def getBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Seq[Row] = {
    if (columns.isEmpty) {
      rows.map { row =>
        val result = get(row, family)
        Row(row, Seq(ColumnFamily(family, result)))
      }.filter(!_.families.head.columns.isEmpty)
    } else {
      val keys = rows.map(ByteBuffer.wrap(_)).asJava
      val parent = new ColumnParent(family)
      val predicate = new SlicePredicate
      columns.foreach { column =>
        predicate.addToColumn_names(ByteBuffer.wrap(column))
      }

      val slices = client.multiget_slice(keys, parent, predicate, consistency)
      slices.asScala.map { case (row, slice) =>
        Row(row.array, Seq(ColumnFamily(family, getColumns(slice))))
      }.toSeq
    }
  }

  override def intraRowScan(row: ByteArray, family: String, startColumn: ByteArray, stopColumn: ByteArray): IntraRowScanner = {
    new IntraRowScanner {
      var iterator = get(row, family, startColumn, stopColumn, 100).iterator

      override def close: Unit = ()

      override def hasNext: Boolean = {
        iterator.hasNext
      }

      override def next: Column = {
        val column = iterator.next
        if (!iterator.hasNext) {
          iterator = get(row, family, column.qualifier, stopColumn, 100).iterator
          iterator.next // get ride of the first one
        }
        column
      }
    }
  }

  private def getColumns(result: java.util.List[ColumnOrSuperColumn]): Seq[Column] = {
    result.asScala.map { column =>
      val c = column.getColumn
      Column(c.getName, c.getValue, c.getTimestamp)
    }
  }

  override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val put = new CassandraColumn(ByteBuffer.wrap(column)).setValue(value)
    if (timestamp != 0) put.setTimestamp(timestamp) else put.setTimestamp(System.currentTimeMillis)
    client.insert(key, parent, put, consistency)
  }

  override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = family
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    createMutationMapEntry(updates, key, parent)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      val put = new ColumnOrSuperColumn
      put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
      put.column.setValue(value)
      if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
      else put.column.setTimestamp(timestamp)
      val mutation = new Mutation
      mutation.column_or_supercolumn = put
      updates.get(key).get(parent).add(mutation)
    }

    client.batch_mutate(updates, consistency)
  }

  override def put(row: ByteArray, families: Seq[ColumnFamily]): Unit = {
    val key = ByteBuffer.wrap(row)
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    families.foreach { case ColumnFamily(family, columns) =>
      createMutationMapEntry(updates, key, family)
      columns.foreach { case Column(qualifier, value, timestamp) =>
        val put = new ColumnOrSuperColumn
        put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
        put.column.setValue(value)
        if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
        else put.column.setTimestamp(timestamp)
        val mutation = new Mutation
        mutation.column_or_supercolumn = put
        updates.get(key).get(family).add(mutation)
      }
    }

    client.batch_mutate(updates, consistency)
  }

  override def putBatch(rows: Row*): Unit = {
    val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]

    val puts = rows.map { case Row(row, families) =>
      val key = ByteBuffer.wrap(row)
      families.foreach { case ColumnFamily(family, columns) =>
        createMutationMapEntry(updates, key, family)
        columns.foreach { case Column(qualifier, value, timestamp) =>
          val put = new ColumnOrSuperColumn
          put.column = new CassandraColumn(ByteBuffer.wrap(qualifier))
          put.column.setValue(value)
          if (timestamp == 0) put.column.setTimestamp(System.currentTimeMillis)
          else put.column.setTimestamp(timestamp)
          val mutation = new Mutation
          mutation.column_or_supercolumn = put
          updates.get(key).get(family).add(mutation)
        }
      }
    }

    client.batch_mutate(updates, consistency)
  }

  override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val key = ByteBuffer.wrap(row)

    if (columns.isEmpty) {
      val path = new ColumnPath(family)
      client.remove(key, path, System.currentTimeMillis, consistency)
    } else if (columns.size == 1) {
      val key = ByteBuffer.wrap(row)
      val path = new ColumnPath(family).setColumn(columns.head)
      client.remove(key, path, System.currentTimeMillis, consistency)
    } else {
      val parent = family
      val updates = new java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]
      createMutationMapEntry(updates, key, parent)

      val predicate = new SlicePredicate
      columns.foreach { column => predicate.addToColumn_names(ByteBuffer.wrap(column)) }

      val deletion = new Deletion
      deletion.setTimestamp(System.currentTimeMillis)
      deletion.setPredicate(predicate)

      val mutation = new Mutation
      mutation.deletion = deletion
      updates.get(key).get(parent).add(mutation)

      client.batch_mutate(updates, consistency)
    }
  }

  override def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    val key = ByteBuffer.wrap(row)

    if (families.isEmpty) {
      columnFamilies.foreach { family =>
        val path = new ColumnPath(family)
        client.remove(key, path, System.currentTimeMillis, consistency)
      }
    } else {
      families.foreach { case (family, columns) =>
        delete(row, family, columns: _*)
      }
    }
  }

  override def deleteBatch(rows: Seq[ByteArray]): Unit = {
    rows.foreach { row =>
      delete(row)
    }
  }

  /** Create mutation map entry if necessary. */
  private def createMutationMapEntry(updates: java.util.HashMap[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]], key: ByteBuffer, family: String) {
    if (!updates.containsKey(key)) {
      val row = new java.util.HashMap[String, java.util.List[Mutation]]
      row.put(family, new java.util.ArrayList[Mutation])
      updates.put(key, row)
    } else if (!updates.get(key).containsKey(family)) {
      updates.get(key).put(family, new java.util.ArrayList[Mutation])
    }
  }

  override def getCounter(row: ByteArray, family: String, column: ByteArray): Long = {
    val key = ByteBuffer.wrap(row)
    val path = new ColumnPath(family).setColumn(column)
    client.get(key, path, consistency).counter_column.value
  }

  override def addCounter(row: ByteArray, family: String, column: ByteArray, value: Long): Unit = {
    val key = ByteBuffer.wrap(row)
    val parent = new ColumnParent(family)
    val counter = new CounterColumn(ByteBuffer.wrap(column), value)
    client.add(key, parent, counter, consistency)
  }

  override def addCounter(row: ByteArray, families: Seq[(String, Seq[(ByteArray, Long)])]): Unit = {
    families.foreach { case (family, columns) =>
      columns.foreach { case (column, value) =>
        addCounter(row, family, column, value)
      }
    }
  }
}
