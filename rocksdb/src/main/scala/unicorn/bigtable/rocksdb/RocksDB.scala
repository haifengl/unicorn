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

package unicorn.bigtable.rocksdb

import java.io.File
import java.util.Properties
import org.rocksdb.{ColumnFamilyDescriptor, Options}
import unicorn.bigtable.Database

/** RocksDB abstraction. RocksDB is an embeddable persistent key-value store
  * for fast storage. There is no concept of tables in RocksDB. In fact, a
  * RocksDB is like a table in HBase. In this class, we create a higher level
  * concept of database that contains multiple RocksDB databases in a directory.
  * Each RocksDB is actually a subdirectory, which is encapsulated in RocksTable.
  *
  * @author Haifeng Li
  */
class RocksDB(val path: String) extends Database[RocksTable] {
  val dir = new File(path)
  require(dir.exists, s"Directory $path doesn't exist")

  override def close: Unit = ()

  override def apply(name: String): RocksTable = {
    new RocksTable(s"$path/$name")
  }

  override def tables: Set[String] = {
    dir.listFiles.filter(_.isDirectory).map(_.getName).toSet
  }

  /** The parameter props is ignored. */
  override def createTable(name: String, props: Properties, families: String*): RocksTable = {
    val options = new Options
    options.setCreateIfMissing(true)
    options.setErrorIfExists(true)
    options.setCreateMissingColumnFamilies(false)

    val rocksdb = org.rocksdb.RocksDB.open(options, s"$path/$name")
    families.foreach { family =>
      val descriptor = new ColumnFamilyDescriptor(family.getBytes)
      rocksdb.createColumnFamily(descriptor)
    }

    rocksdb.close
    new RocksTable(s"$path/$name")
  }
  
  override def dropTable(name: String): Unit = {
    new File(s"$path/$name").delete
  }

  override def truncateTable(name: String): Unit = {
    throw new UnsupportedOperationException("RocksDB doesn't support truncateTable")
  }

  override def tableExists(name: String): Boolean = {
    val options = new Options().setCreateIfMissing(false)
    try {
      org.rocksdb.RocksDB.open(options, s"$path/$name")
      true
    } catch {
      case _: Exception => false
    }
  }

  override def compactTable(name: String): Unit = {
    org.rocksdb.RocksDB.open(s"$path/$name").compactRange
  }
}

object RocksDB {

  /** Creates a RocksDB database.
    *
    * @param path path to database.
    */
  def create(path: String): RocksDB = {
    val dir = new java.io.File(path)
    require(!dir.exists, s"Directory $path exists")

    dir.mkdir
    new RocksDB(path)
  }

  def apply(path: String): RocksDB = {
    new RocksDB(path)
  }
}
