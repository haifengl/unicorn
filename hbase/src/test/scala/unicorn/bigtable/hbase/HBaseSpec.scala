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

package unicorn.bigtable.hbase

import java.nio.ByteBuffer

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable._

/**
 * @author Haifeng Li
 */
class HBaseSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val hbase = HBase()
  val tableName = "unicorn_test"
  var table: HBaseTable = null

  override def beforeAll = {
    hbase.createTable(tableName, "cf1", "cf2")
    table = hbase(tableName)
  }

  override def afterAll = {
    if (table != null) table.close
    hbase.dropTable(tableName)
  }

  "HBase" should {
    "get the put" in {
      table.put("row1", "cf1", "c1", "v1", 0L)
      new String(table("row1", "cf1", "c1").get, UTF8) === "v1"
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }

    "get the family" in {
      table.put("row1", "cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))
      val columns = table.get("row1", "cf1")
      columns.size === 2
      new String(columns(0).value, UTF8) === "v1"
      new String(columns(1).value, UTF8) === "v2"

      table.delete("row1", "cf1")
      val empty = table.get("row1", "cf1")
      empty.size === 0
    }

    "get empty family" in {
      val columns = table.get("row1", "cf1")
      columns.size === 0
    }

    "get nonexistent family" in {
      table.get("row1", "cf5") must throwA[Exception]
    }

    "get the row" in {
      table.put("row1", Seq(
        ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2"))),
        ColumnFamily("cf2", Seq(Column("c3", "v3"))))
      )
      val families = table.get("row1")
      families.size === 2
      families(0).columns.size === 2
      families(1).columns.size === 1
      families(0).family === "cf1"
      families(1).family === "cf2"

      new String(families(0).columns(0).value, UTF8) === "v1"
      new String(families(0).columns(1).value, UTF8) === "v2"
      new String(families(1).columns(0).value, UTF8) === "v3"

      table.delete("row1", "cf1")
      val cf1 = table.get("row1", "cf1")
      cf1.size === 0

      table.get("row1").size === 1
      val cf2 = table.get("row1", "cf2")
      cf2.size === 1

      table.delete("row1")
      table.get("row1").size === 0
    }

    "get nonexistent row" in {
      val families = table.get("row5")
      families.size === 0
    }

    "get multiple rows" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2"))),
           ColumnFamily("cf2", Seq(Column("c3", "v3")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))))

      table.putBatch(row1, row2)

      val keys = Seq("row1", "row2")
      val rows = table.getBatch(keys)
      rows.size === 2
      rows(0).families.size === 2
      rows(1).families.size === 1

      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "scan" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2"))),
          ColumnFamily("cf2", Seq(Column("c3", "v3")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))))

      val row3 = Row("row3",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))))

      table.putBatch(row1, row2, row3)

      val scanner = table.scan("row1", "row3")
      val r1 = scanner.next
      new String(r1.key, UTF8) === "row1"
      r1.families(0).family === "cf1"
      r1.families(1).family === "cf2"

      val r2 = scanner.next
      new String(r2.key, UTF8) === "row2"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3")
      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "prefix row scan" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2"))),
          ColumnFamily("cf2", Seq(Column("c3", "v3")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))))

      val row3 = Row("row3",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "v1"), Column("c2", "v2")))))

      table.putBatch(row1, row2, row3)

      val prefix = "row"
      val scanner = table.scanPrefix(prefix)
      val r1 = scanner.next
      new String(r1.key, UTF8) === "row1"
      val r2 = scanner.next
      new String(r2.key, UTF8) === "row2"
      val r3 = scanner.next
      new String(r3.key, UTF8) === "row3"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3")
      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "scan with basic filter" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "11"), Column("c2", "12"))),
          ColumnFamily("cf2", Seq(Column("c3", "13")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "21"), Column("c2", "22")))))

      val row3 = Row("row3",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "31"), Column("c2", "32")))))

      table.putBatch(row1, row2, row3)

      val prefix = "row"
      import ScanFilter.CompareOperator._
      val filter = ScanFilter.BasicExpression(Equal, "cf1", "c1", "21")
      val scanner = table.scanPrefix(filter, prefix)
      val r1 = scanner.next
      new String(r1.key, UTF8) === "row2"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3")
      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "scan with and filter" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "11"), Column("c2", "12"))),
          ColumnFamily("cf2", Seq(Column("c3", "13")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "21"), Column("c2", "22")))))

      val row3 = Row("row3",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "31"), Column("c2", "32")))))

      table.putBatch(row1, row2, row3)

      val prefix = "row"
      import ScanFilter.CompareOperator._
      val filter = ScanFilter.And(Seq(
        ScanFilter.BasicExpression(Greater, "cf1", "c1", "11"),
        ScanFilter.BasicExpression(Greater, "cf1", "c2", "22"))
      )
      val scanner = table.scanPrefix(filter, prefix)
      val r1 = scanner.next
      new String(r1.key, UTF8) === "row3"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3")
      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "scan with or filter" in {
      val row1 = Row("row1",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "11"), Column("c2", "12"))),
          ColumnFamily("cf2", Seq(Column("c3", "13")))))

      val row2 = Row("row2",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "21"), Column("c2", "22")))))

      val row3 = Row("row3",
        Seq(ColumnFamily("cf1", Seq(Column("c1", "31"), Column("c2", "32")))))

      table.putBatch(row1, row2, row3)

      val prefix = "row"
      import ScanFilter.CompareOperator._
      val filter = ScanFilter.Or(Seq(
        ScanFilter.BasicExpression(Less, "cf1", "c1", "21"),
        ScanFilter.BasicExpression(GreaterOrEqual, "cf1", "c1", "31"))
      )
      val scanner = table.scanPrefix(filter, prefix)
      val r1 = scanner.next
      new String(r1.key, UTF8) === "row1"
      val r3 = scanner.next
      new String(r3.key, UTF8) === "row3"
      scanner.hasNext === false
      scanner.close

      val keys = Seq("row1", "row2", "row3")
      table.deleteBatch(keys)
      table.getBatch(keys).size === 0
    }

    "intra row scan" in {
      table.put("row1".getBytes(UTF8), Seq(
        ColumnFamily("cf1", (1 to 1000).map { i =>
          val bytes = ByteBuffer.allocate(4).putInt(i).array
          Column(bytes, bytes)
        }))
      )

      val b103 = ByteBuffer.allocate(4).putInt(103).array
      val b415 = ByteBuffer.allocate(4).putInt(415).array
      val iterator = table.intraRowScan("row1", "cf1", b103, b415)
      (103 to 415).foreach { i =>
        iterator.hasNext === true
        val column = iterator.next
        ByteBuffer.wrap(column.qualifier).getInt === i
        ByteBuffer.wrap(column.value).getInt === i
      }

      iterator.hasNext === false
      table.delete("row1")
      table.get("row1").size === 0
    }

    "rollback" in {
      table.put("row1", "cf1", "c1", "v1", 0L)
      table.put("row1", "cf1", "c1", "v2", 0L)
      new String(table("row1", "cf1", "c1").get, UTF8) === "v2"
      table.rollback("row1", "cf1", "c1")
      new String(table("row1", "cf1", "c1").get, UTF8) === "v1"
      table.rollback("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }

    "append" in {
      table.put("row1", "cf1", "c1", "v1", 0L)
      table.append("row1", "cf1", "c1", "v2")
      new String(table("row1", "cf1", "c1").get, UTF8) === "v1v2"
      table.delete("row1", "cf1", "c1")
      table("row1", "cf1", "c1") === None
    }

    "counter" in {
      table.getCounter("row1", "cf1", "counter") === 0
      table.addCounter("row1", "cf1", "counter", 10)
      table.getCounter("row1", "cf1", "counter") === 10
      table.addCounter("row1", "cf1", "counter", -5)
      table.getCounter("row1", "cf1", "counter") === 5
      table.delete("row1", "cf1", "counter")
      table("row1", "cf1", "counter") === None
    }
  }
}
