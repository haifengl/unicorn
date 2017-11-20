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

package unicorn.bigtable.rocksdb

import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, Options, ReadOptions, WriteBatch, WriteOptions}
import unicorn.bigtable._

/** A RocksTable is actually a plain RocksDB in its own directory.
  * RocksDB is a key-value store. Although we add the abstraction of
  * rows and columns, we don't support timestamps. The timestamp
  * parameters in the API are all ignored. The timestamp in the
  * returned values are always 0L.
  *
  * @author Haifeng Li
  */
class RocksTable(val path: String, val options: DBOptions = new DBOptions) extends BigTable {
  options.setCreateIfMissing(false)
  options.setCreateMissingColumnFamilies(false)

  val name: String = new java.io.File(path).getName

  private val listColumnFamilies = org.rocksdb.RocksDB.listColumnFamilies(new Options().setCreateIfMissing(false), path).asScala

  override val columnFamilies = listColumnFamilies.map(new String(_)).filter(_ != "default")

  val descriptors = listColumnFamilies.map(new ColumnFamilyDescriptor(_)).asJava

  private def open: (org.rocksdb.RocksDB, Map[String, ColumnFamilyHandle]) = {
    val handles = new java.util.ArrayList[ColumnFamilyHandle]
    val rocksdb = org.rocksdb.RocksDB.open(options, path, descriptors, handles)
    val map = columnFamilies.zip(handles.asScala).toMap
    (rocksdb, map)
  }

  val (rocksdb, handles) = open

  override def close: Unit = {
    rocksdb.close
    options.close
  }

  private val keyBuffer = ByteBuffer.allocate(65536)

  /** Returns the key of a (row, column) pair. */
  def key(row: ByteArray, column: ByteArray): Array[Byte] = {
    keyBuffer.clear
    keyBuffer.putShort(row.length.toShort)
    keyBuffer.put(row.bytes)
    keyBuffer.putShort(column.length.toShort)
    keyBuffer.put(column.bytes)
    keyBuffer
  }

  /** Returns the key prefix of a row. */
  def key(row: ByteArray): Array[Byte] = {
    keyBuffer.clear
    keyBuffer.putShort(row.length.toShort)
    keyBuffer.put(row.bytes)
    keyBuffer
  }

  /** Unzips a key into (row, column) pair. */
  def unzip(key: Array[Byte]): (Array[Byte], Array[Byte]) = {
    val buffer = ByteBuffer.wrap(key)

    val row = new Array[Byte](buffer.getShort)
    buffer.get(row)

    val column = new Array[Byte](buffer.getShort)
    buffer.get(column)

    (row, column)
  }

  override def apply(row: ByteArray, family: String, column: ByteArray): Option[ByteArray] = {
    Option(rocksdb.get(handles(family), key(row, column))).map(ByteArray(_))
  }

  override def get(row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    if (columns.isEmpty) {
      val prefix = key(row)
      val values = new scala.collection.mutable.ArrayBuffer[Column](columns.size)
      val it = rocksdb.newIterator(handles(family))
      it.seek(prefix)
      while (it.isValid) {
        val key = it.key
        if (key.startsWith(prefix)) {
          val (_, column) = unzip(key)
          values += Column(column, it.value)
          it.next
        } else {
          return values
        }
      }
      values
    } else {
      val keys = columns.map(key(row, _))
      rocksdb.multiGet(List.fill(keys.size)(handles(family)).asJava, keys.asJava).asScala.map { case (key, value) =>
        val (_, column) = unzip(key)
        Column(column, value)
      }.toSeq
    }
  }

  override def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily] = {
    (if (families.isEmpty) {
      columnFamilies.map { family =>
        ColumnFamily(family, get(row, family))
      }
    } else {
      families.map { case (family, columns) =>
        ColumnFamily(family, get(row, family, columns: _*))
      }
    }).filter(!_.columns.isEmpty)
  }

  override def getBatch(rows: Seq[ByteArray], families: Seq[(String, Seq[ByteArray])]): Seq[Row] = {
    rows.map { row =>
      Row(row, get(row, families))
    }.filter(!_.families.isEmpty)
  }

  override def getBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Seq[Row] = {
    rows.map { row =>
      Row(row, Seq(ColumnFamily(family, get(row, family, columns: _*))))
    }.filter(!_.families.isEmpty)
  }

  override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit = {
    rocksdb.put(handles(family), key(row, column), value)
  }

  override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val batch = new WriteBatch
    columns.foreach { column =>
      batch.put(handles(family), key(row, column.qualifier), column.value)
    }
    rocksdb.write(new WriteOptions, batch)
  }

  override def put(row: ByteArray, families: Seq[ColumnFamily]): Unit = {
    val batch = new WriteBatch
    families.foreach { case ColumnFamily(family, columns) =>
      columns.foreach { column =>
        batch.put(handles(family), key(row, column.qualifier), column.value)
      }
    }
    rocksdb.write(new WriteOptions, batch)
  }

  override def putBatch(rows: Row*): Unit = {
    rows.foreach { case Row(row, families) =>
      put(row, families)
    }
  }

  override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    if (columns.isEmpty) {
      val prefix = key(row)
      val it = rocksdb.newIterator(handles(family))
      val batch = new WriteBatch
      it.seek(prefix)
      while (it.isValid) {
        val key = it.key
        if (key.startsWith(prefix)) {
          batch.remove(handles(family), key)
          it.next
        } else {
          rocksdb.write(new WriteOptions, batch)
          return
        }
      }
      rocksdb.write(new WriteOptions, batch)
    } else {
      val batch = new WriteBatch
      columns.foreach { column =>
        batch.remove(handles(family), key(row, column))
      }
      rocksdb.write(new WriteOptions, batch)
    }
  }

  override def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    if (families.isEmpty) {
      columnFamilies.foreach { family =>
        delete(row, family)
      }
    } else {
      families.foreach { case (family, columns) =>
        delete(row, family, columns: _*)
      }
    }
  }

  override def deleteBatch(rows: Seq[ByteArray]): Unit = {
    rows.foreach { row =>
      columnFamilies.foreach { family =>
         delete(row, family)
      }
    }
  }
}
