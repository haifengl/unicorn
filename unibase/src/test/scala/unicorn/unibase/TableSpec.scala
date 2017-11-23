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

package unicorn.unibase

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable.accumulo.Accumulo
import unicorn.json._

/**
 * @author Haifeng Li
 */
class TableSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val bigtable = Accumulo()
  val db = new Unibase(bigtable)
  val tableName = "unicorn_unibase_test"
  val json = JsonParser(
    """
      |{
      |  "owner": "Rich",
      |  "phone": "123-456-7890",
      |  "address": {
      |    "street": "1 ADP Blvd.",
      |    "city": "Roseland",
      |    "state": "NJ"
      |  },
      |  "store": {
      |    "book": [
      |      {
      |        "category": "reference",
      |        "author": "Nigel Rees",
      |        "title": "Sayings of the Century",
      |        "price": 8.95
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Evelyn Waugh",
      |        "title": "Sword of Honour",
      |        "price": 12.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "Herman Melville",
      |        "title": "Moby Dick",
      |        "isbn": "0-553-21311-3",
      |        "price": 8.99
      |      },
      |      {
      |        "category": "fiction",
      |        "author": "J. R. R. Tolkien",
      |        "title": "The Lord of the Rings",
      |        "isbn": "0-395-19395-8",
      |        "price": 22.99
      |      }
      |    ],
      |    "bicycle": {
      |      "color": "red",
      |      "price": 19.95
      |    }
      |  }
      |}
    """.stripMargin).asInstanceOf[JsObject]

  val key = ObjectId()
  json("_id") = key

  override def beforeAll = {
    db.createTable(tableName)
  }

  override def afterAll = {
    db.drop(tableName)
  }

  "Table" should {
    "upsert, get, remove" in {
      val bucket = db.table(tableName)
      bucket.upsert(json)

      val obj = bucket(key)
      obj.get === json

      bucket.delete(key)
      bucket(key) === None
    }
    /*
    "insert" in {
      val bucket = db.table(tableName)
      bucket.upsert(json)
      bucket.insert(json) === false
    }
    */
    "update.set" in {
      val bucket = db.table(tableName)

      val update = JsonParser(
        """
          | {
          |   "$set": {
          |     "owner": "Poor",
          |     "gender": "M"
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]

      bucket.update(key, update)

      val doc = bucket(key, "owner", "gender").get
      doc.owner === JsString("Poor")
      doc.gender === JsString("M")
    }
    "update.unset" in {
      val bucket = db.table(tableName)

      val update = JsonParser(
        """
          | {
          |   "$unset": {
          |     "owner": 1,
          |     "address": 1
          |   }
          | }
        """.stripMargin).asInstanceOf[JsObject]

      bucket.update(key, update)

      bucket(key, "owner", "address") === None
    }
  }
}
