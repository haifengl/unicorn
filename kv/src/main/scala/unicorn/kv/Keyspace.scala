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

package unicorn.kv

case class KeyValue(key: Array[Byte], value: Array[Byte])

object KeyValue {
  def apply(kv: Tuple2[Array[Byte], Array[Byte]]): KeyValue = {
    KeyValue(kv._1, kv._2)
  }
}

/** A keyspace is a set key-value pairs.
  *
  * @author Haifeng Li
  */
trait Keyspace extends AutoCloseable {
  /** Table name. */
  val name: String

  override def toString = name
  override def hashCode = toString.hashCode

  /** Get a value. */
  def apply(key: Array[Byte]): Option[Array[Byte]] = {
    get(key)
  }

  /** Update a value. With it, one may use the syntactic sugar
    * ```
    * table(key) = value
    * ```
    */
  def update(key: Array[Byte], value: Array[Byte]): Unit = {
    put(key, value)
  }

  /** Get a value. */
  def get(key: Array[Byte]): Option[Array[Byte]]

  /** Get multiple key-value pairs.
    * The implementation may or may not optimize the batch operations.
    */
  def getBatch(keys: Seq[Array[Byte]]): Seq[KeyValue] = {
    keys.map { key => (key, get(key)) }
      .filter(_._2.isDefined)
      .map { pair => KeyValue(pair._1, pair._2.get) }
  }

  /** Upsert a value. */
  def put(key: Array[Byte], value: Array[Byte]): Unit

  /** Upsert values. */
  def put(kv: KeyValue): Unit = {
    put(kv.key, kv.value)
  }

  /** Put multiple key-value pairs.
    */
  def putBatch(kv: KeyValue*): Unit = {
    kv.foreach(put(_))
  }

  /** Delete a key-value pair. */
  def delete(key: Array[Byte]): Unit

  /** Delete multiple rows.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def deleteBatch(keys: Seq[Array[Byte]]): Unit = {
    keys.foreach(delete(_))
  }
}

/** Key-value iterator */
trait KeyValueIterator extends Iterator[KeyValue] with AutoCloseable {
  def close: Unit
}

/** If the key is ordered, we can scan a range of keys. */
trait OrderedKeyspace extends Keyspace {
  /** Start row in a table. */
  val TableStartKey: Array[Byte]
  /** End row in a table. */
  val TableEndKey: Array[Byte]

  /** Scan the whole space.
    */
  def scan: Iterator[KeyValue]

  /** Scan a range of keys.
    * @param startRow row to start scanner at or after (inclusive)
    * @param endRow row to stop scanner before (exclusive)
    */
  def scan(start: Array[Byte], end: Array[Byte]): Iterator[KeyValue]

  /** Scan the keys that starts with the given prefix. */
  def scan(prefix: Array[Byte]): Iterator[KeyValue] = {
    scan(prefix, prefixEndKey(prefix, TableEndKey))
  }
}
