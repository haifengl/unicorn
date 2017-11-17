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

package unicorn.bigtable.hbase

import java.util.Date

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Append, Delete, Get, Increment, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.{ColumnPrefixFilter, MultipleColumnPrefixFilter, ColumnRangeFilter, CompareFilter, FilterList, KeyOnlyFilter, SingleColumnValueFilter}, CompareFilter.CompareOp
import org.apache.hadoop.hbase.security.visibility.{Authorizations, CellVisibility}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HConstants}
import unicorn.bigtable._, ScanFilter._
import unicorn.util._

/** HBase table adapter.
  *
  * @author Haifeng Li
  */
class HBaseTable(val db: HBase, val name: String) extends BigTable with FilterScan with IntraRowScan with TimeTravel with CheckAndPut with CellLevelSecurity with Appendable with Rollback with Counter {

  val table = db.connection.getTable(TableName.valueOf(name))

  override def close: Unit = table.close

  override val columnFamilies = table.getTableDescriptor.getColumnFamilies.map(_.getNameAsString).toSeq

  override val startRowKey: ByteArray = HConstants.EMPTY_START_ROW
  override val endRowKey: ByteArray = HConstants.EMPTY_END_ROW

  var cellVisibility: Option[CellVisibility] = None
  var authorizations: Option[Authorizations] = None

  override def setCellVisibility(expression: String): Unit = {
    cellVisibility = Some(new CellVisibility(expression))
  }

  override def getCellVisibility: String = {
    cellVisibility.map(_.getExpression).getOrElse("")
  }

  override def setAuthorizations(labels: String*): Unit = {
    authorizations = Some(new Authorizations(labels: _*))
  }

  override def getAuthorizations: Seq[String] = {
    if (authorizations.isDefined) authorizations.get.getLabels.asScala
    else Seq.empty
  }

  override def apply(row: ByteArray, family: String, column: ByteArray): Option[ByteArray] = {
    val get = newGet(row)
    get.addColumn(family, column)
    Option(table.get(get).getValue(family, column)).map(ByteArray(_))
  }

  private def getColumns(get: Get, family: String, columns: Seq[ByteArray]): Unit = {
    if (columns.isEmpty)
      get.addFamily(family)
    else
      columns.foreach { column => get.addColumn(family, column) }
  }

  private def scanColumns(scan: Scan, family: String, columns: Seq[ByteArray]): Unit = {
    if (columns.isEmpty)
      scan.addFamily(family)
    else
      columns.foreach { column => scan.addColumn(family, column) }
  }

  override def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily] = {
    val get = newGet(row)
    families.foreach { case (family, columns) => getColumns(get, family, columns) }
    HBaseTable.getRow(table.get(get)).families
  }

  override def get(row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    val get = newGet(row)
    getColumns(get, family, columns)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq.empty else result.families.head.columns
  }

  override def getBatch(rows: Seq[ByteArray], families: Seq[(String, Seq[ByteArray])]): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      families.foreach { case (family, columns) => getColumns(get, family, columns) }
      get
    }

    HBaseTable.getRows(table.get(gets.asJava))
  }

  override def getBatch(rows: Seq[ByteArray], family: String, columns: ByteArray*): Seq[Row] = {
    val gets = rows.map { row =>
      val get = newGet(row)
      getColumns(get, family, columns)
      get
    }

    HBaseTable.getRows(table.get(gets.asJava))
  }

  override def getAsOf(asOfDate: Date, row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    val get = newGet(row)
    get.setTimeRange(0, asOfDate.getTime)
    getColumns(get, family, columns)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq.empty else result.families.head.columns
  }

  override def getAsOf(asOfDate: Date, row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Seq[ColumnFamily] = {
    val get = newGet(row)
    get.setTimeRange(0, asOfDate.getTime)
    families.foreach { case (family, columns) => getColumns(get, family, columns) }
    HBaseTable.getRow(table.get(get)).families
  }

  override def scan(startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    val scan = newScan(startRow, stopRow)
    families.foreach { case (family, columns) => scanColumns(scan, family, columns) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def scan(startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
    val scan = newScan(startRow, stopRow)
    scanColumns(scan, family, columns)
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    val scan = newScan(startRow, stopRow)
    scan.setFilter(hbaseFilter(filter))
    families.foreach { case (family, columns) => scanColumns(scan, family, columns) }
    new HBaseRowScanner(table.getScanner(scan))
  }

  private[unicorn] def hbaseScan(startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])], filter: Option[ScanFilter.Expression] = None): Scan = {
    val scan = newScan(startRow, stopRow)
    if (filter.isDefined) scan.setFilter(hbaseFilter(filter.get))
    families.foreach { case (family, columns) => scanColumns(scan, family, columns) }
    scan
  }

  override def filterScan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: ByteArray*): RowScanner = {
    val scan = newScan(startRow, stopRow)
    scan.setFilter(hbaseFilter(filter))
    scanColumns(scan, family, columns)
    new HBaseRowScanner(table.getScanner(scan))
  }

  override def filterGet(filter: ScanFilter.Expression, row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Option[Seq[ColumnFamily]] = {
    val get = newGet(row)
    get.setFilter(hbaseFilter(filter))
    families.foreach { case (family, columns) => getColumns(get, family, columns) }
    val result = HBaseTable.getRow(table.get(get)).families
    if (result.isEmpty) None else Some(result)
  }

  override def filterGet(filter: ScanFilter.Expression, row: ByteArray, family: String, columns: ByteArray*): Option[Seq[Column]] = {
    val get = newGet(row)
    get.setFilter(hbaseFilter(filter))
    getColumns(get, family, columns)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) None else Some(result.families.head.columns)
  }

  val keyOnlyFilter = new KeyOnlyFilter(true)

  override def getKeyOnly(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily] = {
    val get = newGet(row)
    get.setFilter(keyOnlyFilter)
    families.foreach { case (family, columns) => getColumns(get, family, columns) }
    HBaseTable.getRow(table.get(get)).families
  }

  override def getKeyOnly(row: ByteArray, family: String, columns: ByteArray*): Seq[Column] = {
    val get = newGet(row)
    get.setFilter(keyOnlyFilter)
    getColumns(get, family, columns)

    val result = HBaseTable.getRow(table.get(get))
    if (result.families.isEmpty) Seq.empty else result.families.head.columns
  }

  private def hbaseFilter(filter: ScanFilter.Expression): org.apache.hadoop.hbase.filter.Filter = filter match {
    case BasicExpression(op, family, column, value, filterIfMissing) =>
      val f = op match {
        case CompareOperator.Equal => new SingleColumnValueFilter(family, column, CompareOp.EQUAL, value)
        case CompareOperator.NotEqual => new SingleColumnValueFilter(family, column, CompareOp.NOT_EQUAL, value)
        case CompareOperator.Greater => new SingleColumnValueFilter(family, column, CompareOp.GREATER, value)
        case CompareOperator.GreaterOrEqual => new SingleColumnValueFilter(family, column, CompareOp.GREATER_OR_EQUAL, value)
        case CompareOperator.Less => new SingleColumnValueFilter(family, column, CompareOp.LESS, value)
        case CompareOperator.LessOrEqual => new SingleColumnValueFilter(family, column, CompareOp.LESS_OR_EQUAL, value)
      }
      f.setFilterIfMissing(filterIfMissing)
      f
    case And(list) => new FilterList(FilterList.Operator.MUST_PASS_ALL, list.map(hbaseFilter(_)).asJava)
    case Or(list) => new FilterList(FilterList.Operator.MUST_PASS_ONE, list.map(hbaseFilter(_)).asJava)
  }

  override def intraRowScan(row: ByteArray, family: String, startColumn: ByteArray, stopColumn: ByteArray): IntraRowScanner = {
    val scan = newColumnRangeScan(row, family, startColumn, stopColumn, 100)
    new HBaseColumnScanner(table.getScanner(scan))
  }

  def prefixColumnScan(row: ByteArray, family: String, prefix: ByteArray*): IntraRowScanner = {
    val scan = newColumnPrefixScan(row, family, prefix.map(_.bytes), 100)
    new HBaseColumnScanner(table.getScanner(scan))
  }

  override def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit = {
    val put = newPut(row)
    if (timestamp != 0) put.addColumn(family, column, timestamp, value) else put.addColumn(family, column, value)
    table.put(put)
  }

  override def put(row: ByteArray, family: String, columns: Column*): Unit = {
    val put = newPut(row)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      if (timestamp == 0)
        put.addColumn(family, qualifier, value)
      else
        put.addColumn(family, qualifier, timestamp, value)
    }
    table.put(put)
  }

  override def put(row: ByteArray, families: Seq[ColumnFamily]): Unit = {
    val put = newPut(row)
    families.foreach { case ColumnFamily(family, columns) =>
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          put.addColumn(family, qualifier, value)
        else
          put.addColumn(family, qualifier, timestamp, value)
      }
    }
    table.put(put)
  }

  override def putBatch(rows: Row*): Unit = {
    val puts = rows.map { case Row(row, families) =>
      val put = newPut(row)
      families.foreach { case ColumnFamily(family, columns) =>
        columns.foreach { case Column(qualifier, value, timestamp) =>
          if (timestamp == 0)
            put.addColumn(family, qualifier, value)
          else
            put.addColumn(family, qualifier, timestamp, value)
        }
      }
      put
    }
    table.put(puts.asJava)
  }

  override def checkAndPut(row: ByteArray, checkFamily: String, checkColumn: ByteArray, family: String, columns: Column*): Boolean = {
    val put = newPut(row)
    columns.foreach { case Column(qualifier, value, timestamp) =>
      if (timestamp == 0)
        put.addColumn(family, qualifier, value)
      else
        put.addColumn(family, qualifier, timestamp, value)
    }
    table.checkAndPut(row, checkFamily, checkColumn, null, put)
  }

  override def checkAndPut(row: ByteArray, checkFamily: String, checkColumn: ByteArray, families: Seq[ColumnFamily]): Boolean = {
    val put = newPut(row)
    families.foreach { case ColumnFamily(family, columns) =>
      columns.foreach { case Column(qualifier, value, timestamp) =>
        if (timestamp == 0)
          put.addColumn(family, qualifier, value)
        else
          put.addColumn(family, qualifier, timestamp, value)
      }
    }
    table.checkAndPut(row, checkFamily, checkColumn, null, put)
  }

  override def delete(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    val deleter = newDelete(row)
    if (columns.isEmpty) deleter.addFamily(family)
    else columns.foreach { column => deleter.addColumns(family, column) }
    table.delete(deleter)
  }

  override def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    if (families.isEmpty) {
      val deleter = newDelete(row)
      columnFamilies.foreach { family =>
        deleter.addFamily(family)
      }
      table.delete(deleter)
    } else {
      val deleter = newDelete(row)
      families.foreach { case (family, columns) =>
        if (columns.isEmpty)
          deleter.addFamily(family)
        else
          columns.foreach { column => deleter.addColumn(family, column) }
      }
      table.delete(deleter)
    }
  }

  override def deleteBatch(rows: Seq[ByteArray]): Unit = {
    val deletes = rows.map(newDelete(_))
    // HTable modifies the input parameter deletes.
    // Make sure we pass in a mutable collection.
    table.delete(deletes.asJava)
  }

  override def rollback(row: ByteArray, family: String, columns: ByteArray*): Unit = {
    require(!columns.isEmpty)

    val deleter = newDelete(row)
    columns.foreach { column => deleter.addColumn(family, column) }
    table.delete(deleter)
  }

  override def rollback(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit = {
    require(!families.isEmpty)

    val deleter = newDelete(row)
    families.foreach { case (family, columns) =>
      require(!columns.isEmpty)
      columns.foreach { column => deleter.addColumn(family, column) }
    }
    table.delete(deleter)
  }

  override def append(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    val append = newAppend(row)
    append.add(family, column, value)
    table.append(append)
  }

  override def addCounter(row: ByteArray, family: String, column: ByteArray, value: Long): Unit = {
    val increment = newIncrement(row)
    increment.addColumn(family, column, value)
    table.increment(increment)
  }

  override def addCounter(row: ByteArray, families: Seq[(String, Seq[(ByteArray, Long)])]): Unit = {
    val increment = newIncrement(row)
    families.foreach { case (family, columns) =>
        columns.foreach { case (column, value) =>
          increment.addColumn(family, column, value)
        }
    }
    table.increment(increment)
  }

  override def getCounter(row: ByteArray, family: String, column: ByteArray): Long = {
    val value = apply(row, family, column)
    value.map { x => Bytes.toLong(x) }.getOrElse(0)
  }

  private def newGet(row: Array[Byte]): Get = {
    val get = new Get(row)
    if (authorizations.isDefined) get.setAuthorizations(authorizations.get)
    get
  }

  /** Creates a HBase Scan object.
    * @param caching Set the number of rows for caching that will be passed to scanners.
    *                Higher caching values will enable faster scanners but will use more memory.
    * @param cacheBlocks When true, default settings of the table and family are used (this will never override
    *                    caching blocks if the block cache is disabled for that family or entirely).
    *                    If false, default settings are overridden and blocks will not be cached
    */
  private def newScan(startRow: Array[Byte], stopRow: Array[Byte], caching: Int = 20, cacheBlocks: Boolean = true): Scan = {
    val scan = new Scan(startRow, stopRow)
    scan.setCacheBlocks(cacheBlocks)
    scan.setCaching(caching)
    if (authorizations.isDefined) scan.setAuthorizations(authorizations.get)
    scan
  }

  /** Create a intra-row scan object.
    * @param batch Set the maximum number of values to return for each call to next() to
    *              avoid getting all columns for the row.
    */
  private def newColumnRangeScan(row: Array[Byte], family: Array[Byte], startColumn: Array[Byte], stopColumn: Array[Byte], batch: Int = 100): Scan = {
    val scan = new Scan(row, row)
    scan.addFamily(family)
    val filter = new ColumnRangeFilter(startColumn, true, stopColumn, true)
    scan.setFilter(filter)
    scan.setBatch(batch)
    if (authorizations.isDefined) scan.setAuthorizations(authorizations.get)
    scan
  }

  /** Create a intra-row scan object for columns that matches particular prefix(s).
    * @param batch Set the maximum number of values to return for each call to next() to
    *              avoid getting all columns for the row.
    */
  private def newColumnPrefixScan(row: Array[Byte], family: Array[Byte], prefix: Seq[Array[Byte]], batch: Int = 100): Scan = {
    require(!prefix.isEmpty, "Empty prefix for column prefix filter")

    val scan = new Scan(row, row)
    scan.addFamily(family)

    val filter = if (prefix.size == 1)
      new ColumnPrefixFilter(prefix(0))
    else
      new MultipleColumnPrefixFilter(prefix.toArray)

    scan.setFilter(filter)
    scan.setBatch(batch)
    if (authorizations.isDefined) scan.setAuthorizations(authorizations.get)
    scan
  }

  private def newPut(row: Array[Byte]): Put = {
    val put = new Put(row)
    if (cellVisibility.isDefined) put.setCellVisibility(cellVisibility.get)
    put
  }

  private def newDelete(row: Array[Byte]): Delete = {
    val del = new Delete(row)
    if (cellVisibility.isDefined) del.setCellVisibility(cellVisibility.get)
    del
  }

  private def newAppend(row: Array[Byte]): Append = {
    val append = new Append(row)
    if (cellVisibility.isDefined) append.setCellVisibility(cellVisibility.get)
    append
  }

  private def newIncrement(row: Array[Byte]): Increment = {
    val increment = new Increment(row)
    if (cellVisibility.isDefined) increment.setCellVisibility(cellVisibility.get)
    increment
  }
}

object HBaseTable {
  def getRow(result: Result): Row = {
    val valueMap = result.getMap.asScala
    if (valueMap == null) return Row(result.getRow, Seq.empty)

    val families = valueMap.map { case (family, columns) =>
      val values = columns.asScala.flatMap { case (column, ver) =>
        ver.asScala.map { case (timestamp, value) =>
          Column(column, value, timestamp)
        }
      }.toSeq
      ColumnFamily(new String(family, utf8), values)
    }.toSeq
    Row(result.getRow, families)
  }

  def getRows(results: Seq[Result]): Seq[Row] = {
    results.map { result =>
      HBaseTable.getRow(result)
    }.filter(!_.families.isEmpty)
  }
}

class HBaseRowScanner(scanner: ResultScanner) extends RowScanner {
  private val iterator = scanner.iterator

  override def close: Unit = scanner.close

  override def hasNext: Boolean = iterator.hasNext

  override def next: Row = {
    HBaseTable.getRow(iterator.next)
  }
}

class HBaseColumnScanner(scanner: ResultScanner) extends IntraRowScanner {
  private val rowIterator = scanner.iterator
  private var cellIterator = if (rowIterator.hasNext) rowIterator.next.listCells.iterator else null

  override def close: Unit = scanner.close

  override def hasNext: Boolean = {
    if (cellIterator == null) return false
    cellIterator.hasNext
  }

  override def next: Column = {
    val cell = cellIterator.next
    if (!cellIterator.hasNext)
      cellIterator = if (rowIterator.hasNext) rowIterator.next.listCells.iterator else null
    Column(CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell), cell.getTimestamp)
  }
}
