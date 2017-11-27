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

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.kv._

/**
 * @author Haifeng Li
 */
class RocksDBSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val db = RocksDB.create("/tmp/unicorn-rocksdb")
  val tableName = "unicorn_test"
  var table: Rockspace = null

  override def beforeAll = {
    db.create(tableName, null)
    table = db(tableName)
  }

  override def afterAll = {
    if (table != null) table.close
    db.drop("unicorn_test")
    new java.io.File("/tmp/unicorn-rocksdb").delete
  }

  "RocksDB" should {
    "get the put" in {
      table.put("row1", "v1")
      new String(table("row1").get, UTF8) === "v1"
      table.delete("row1")
      table("row1") === None
    }

    "scan" in {
      table.put("foo1", "v1")
      table.put("foo2", "v1")
      table.put("foo3", "v1")
      table.put("bar1", "v1")
      table.put("bar2", "v1")
      table.put("bar3", "v1")

      val it = table.scan("bar")
      new String(it.next.key, UTF8) === "bar1"
      new String(it.next.key, UTF8) === "bar2"
      new String(it.next.key, UTF8) === "bar3"
      it.hasNext === false
    }
  }
}
