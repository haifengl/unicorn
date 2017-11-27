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

package unicorn.kv.rocksdb

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes
import java.util.Properties
import org.rocksdb.Options
import unicorn.kv.KeyValueStore

/** RocksDB abstraction. RocksDB is an embeddable persistent key-value store
  * for fast storage. There is no concept of tables in RocksDB. In fact, a
  * RocksDB is like a table in HBase. In this class, we create a higher level
  * concept of database that contains multiple RocksDB databases in a directory.
  * Each RocksDB is actually a subdirectory, which is encapsulated in RocksTable.
  *
  * @author Haifeng Li
  */
class RocksDB(val path: String) extends KeyValueStore[Rockspace] {
  val dir = new File(path)
  require(dir.exists, s"Directory $path doesn't exist")

  override def close: Unit = {

  }

  override def apply(name: String): Rockspace = {
    new Rockspace(s"$path/$name")
  }

  /** The parameter props is ignored. */
  override def create(name: String, props: Properties): Unit = {
    val options = new Options
    options.setCreateIfMissing(true)
    options.setErrorIfExists(true)
    options.setCreateMissingColumnFamilies(false)

    val rocksdb = org.rocksdb.RocksDB.open(options, s"$path/$name")
    rocksdb.close
  }
  
  override def drop(name: String): Unit = {
    delete(Paths.get(path, name))
  }

  /** Delete directory recursively. */
  private def delete(root: Path): Unit = {
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }

  override def tables: Set[String] = {
    new File(path).list().toSet
  }

  override def exists(name: String): Boolean = {
    new File(s"$path/$name").exists()
  }

  def compact(name: String): Unit = {
    org.rocksdb.RocksDB.open(s"$path/$name").compactRange
  }
}

object RocksDB {

  // loads the RocksDB C++ library.
  org.rocksdb.RocksDB.loadLibrary()

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
