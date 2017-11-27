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

import org.rocksdb.{Options, ReadOptions, WriteBatch, WriteOptions}
import unicorn.kv._

/** A Rockspace is actually a plain RocksDB in its own directory.
  *
  * @author Haifeng Li
  */
class Rockspace(val path: String, val options: Options = new Options) extends OrderedKeyspace {
  val name: String = new java.io.File(path).getName

  options.setCreateIfMissing(false)
  options.setCreateMissingColumnFamilies(false)

  val rocksdb = org.rocksdb.RocksDB.open(options, path)

  override val TableEndKey: Array[Byte] = null
  override val TableStartKey: Array[Byte] = null

  override def close: Unit = {
    rocksdb.close
    options.close
  }

  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    Option(rocksdb.get(key))
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    rocksdb.put(key, value)
  }

  override def putBatch(kvs: KeyValue*): Unit = {
    val batch = new WriteBatch
    kvs.foreach { kv => batch.put(kv.key, kv.value) }
    rocksdb.write(new WriteOptions, batch)
  }

  override def delete(key: Array[Byte]): Unit = {
    rocksdb.delete(key)
  }

  override def deleteBatch(keys: Seq[Array[Byte]]): Unit = {
    val batch = new WriteBatch
    keys.foreach { key => batch.remove(key) }
    rocksdb.write(new WriteOptions, batch)
  }

  override def scan: Iterator[KeyValue] = {
    val it = rocksdb.newIterator
    it.seekToFirst()
    new Iterator[KeyValue] {
      override def hasNext: Boolean = it.isValid
      override def next: KeyValue = {
        KeyValue(it.key, it.value())
      }
    }
  }

  override def scan(start: Array[Byte], end: Array[Byte]): Iterator[KeyValue] = {
    val options = new ReadOptions
    //Compiler doesn't recognize setIterateUpperBound. why???
    //options.setIterateUpperBound(new Slice(end))
    val it = rocksdb.newIterator(options)
    it.seek(start)
    new Iterator[KeyValue] {
      override def hasNext: Boolean = {
        it.isValid && (compareByteArray(it.key, end) < 0)
      }

      override def next: KeyValue = {
        val kv = KeyValue(it.key, it.value())
        it.next
        kv
      }
    }
  }
}
