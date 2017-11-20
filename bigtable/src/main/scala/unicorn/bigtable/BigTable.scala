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

package unicorn.bigtable

import java.util.Date

/** Key of cell */
case class CellKey(row: ByteArray, family: String, qualifier: ByteArray, timestamp: Long)
/** Cell in wide columnar table */
case class Cell(row: ByteArray, family: String, qualifier: ByteArray, value: ByteArray, timestamp: Long = 0)
/** A column of a column family */
case class Column(qualifier: ByteArray, value: ByteArray, timestamp: Long = 0)
/** A column family */
case class ColumnFamily(family: String, columns: Seq[Column])
/** A row */
case class Row(key: ByteArray, families: Seq[ColumnFamily])

/** Abstraction of wide columnar data table.
  *
  * @author Haifeng Li
  */
trait BigTable extends AutoCloseable {
  /** Table name. */
  val name: String

  /** Column families in this table. */
  val columnFamilies: Seq[String]

  override def toString = name + columnFamilies.mkString("[", ",", "]")
  override def hashCode = toString.hashCode

  /** Get a column value. */
  def apply(row: ByteArray, family: String, column: ByteArray): Option[ByteArray] = {
    val seq = get(row, family, Seq(column))
    if (seq.isEmpty) None else Some(seq.head.value)
  }

  /** Update a value. With it, one may use the syntactic sugar
    * table(row, family, column) = value
    */
  def update(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit = {
    put(row, family, column, value, System.currentTimeMillis)
  }

  /** Get one or more columns of a column family. If columns is empty, get all columns in the column family. */
  def get(row: ByteArray, family: String, columns: Seq[ByteArray]): Seq[Column]

  /** Get a column family. */
  def get(row: ByteArray, family: String): Seq[Column] = {
    get(row, family, Seq.empty)
  }

  /** Get all columns in one or more column families. If families is empty, get all column families. */
  def get(row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Seq[ColumnFamily]

  /** Get multiple rows for given column family.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[ByteArray], family: String): Seq[Row] = {
    getBatch(rows, family, Seq.empty)
  }

  /** Get multiple rows for given columns. If columns is empty, get all columns of the column family.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[ByteArray], family: String, columns: Seq[ByteArray]): Seq[Row]

  /** Get multiple rows for given column families. If families is empty, get all column families.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[ByteArray], families: Seq[(String, Seq[ByteArray])] = Seq.empty): Seq[Row]

  /** Upsert a value. */
  def put(row: ByteArray, family: String, column: ByteArray, value: ByteArray, timestamp: Long): Unit

  /** Upsert values. */
  def put(row: ByteArray, family: String, columns: Seq[Column]): Unit

  /** Upsert values. */
  def put(row: ByteArray, families: Seq[ColumnFamily]): Unit

  /** Update the values of one or more rows.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def putBatch(rows: Row*): Unit

  /** Delete the column family of a row. */
  def delete(row: ByteArray, family: String): Unit = {
    delete(row, family, Seq.empty)
  }

  /** Delete the columns of a row. */
  def delete(row: ByteArray, family: String, column: ByteArray): Unit = {
    delete(row, family, Seq(column))
  }

  /** Delete the columns of a row. If columns is empty, delete all columns in the family. */
  def delete(row: ByteArray, family: String, columns: Seq[ByteArray]): Unit

  /** Delete the columns of a row. If families is empty, delete the whole row. */
  def delete(row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Unit

  /** Delete multiple rows.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def deleteBatch(rows: Seq[ByteArray]): Unit
}

/** Get a row at a given time point. */
trait TimeTravel {
  /** Get a column family. If columns is empty, get all columns in the column family. */
  def getAsOf(asOfDate: Date, row: ByteArray, family: String): Seq[Column] = {
    getAsOfDate(asOfDate, row, family, Seq.empty)
  }

  /** Get one or more columns of a column family. If columns is empty, get all columns in the column family. */
  def getAsOfDate(asOfDate: Date, row: ByteArray, family: String, columns: Seq[ByteArray]): Seq[Column]

  /** Get all columns in one or more column families. If families is empty, get all column families. */
  def getAsOfDate(asOfDate: Date, row: ByteArray, families: Seq[(String, Seq[ByteArray])] = Seq.empty): Seq[ColumnFamily]

}

/** Check and put. Put a row only if the given column doesn't exist. */
trait CheckAndPut {
  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: ByteArray, checkFamily: String, checkColumn: ByteArray, family: String, column: Column): Boolean = {
    checkAndPut(row, checkFamily, checkColumn, family, Seq(column))
  }

  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: ByteArray, checkFamily: String, checkColumn: ByteArray, family: String, columns: Seq[Column]): Boolean

  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: ByteArray, checkFamily: String, checkColumn: ByteArray, families: Seq[ColumnFamily]): Boolean
}

/** Row scan iterator */
trait RowScanner extends Iterator[Row] {
  def close: Unit
}

/** If BigTable supports row scan. */
trait RowScan {
  /** First row in a table. */
  val TableStartRow: ByteArray
  /** Last row in a table. */
  val TableEndRow: ByteArray

  /** When scanning for a prefix the scan should stop immediately after the the last row that
    * has the specified prefix. This method calculates the closest next row key immediately following
    * the given prefix.
    *
    * To scan rows with a given prefix, do
    * {{{
    * scan(prefix, nextRowKeyForPrefix(prefix))
    * }}}
    *
    * @param prefix the row key prefix.
    * @return the closest next row key immediately following the given prefix.
    */
  def nextRowKeyForPrefix(prefix: Array[Byte]): Array[Byte] = {
    val ff:  Byte = 0xFF.toByte
    val one: Byte = 1

    // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
    // Search for the place where the trailing 0xFFs start
    val offset = prefix.reverse.indexOf(ff) match {
      case -1 => prefix.length
      case  x => prefix.length - x - 1
    }

    // We got an 0xFFFF... (only FFs) stopRow value which is
    // the last possible prefix before the end of the table.
    // So set it to stop at the 'end of the table'
    if (offset == 0) {
      return TableEndRow
    }

    // Copy the right length of the original
    val stopRow = java.util.Arrays.copyOfRange(prefix, 0, offset)
    // And increment the last one
    stopRow(stopRow.length - 1) = (stopRow(stopRow.length - 1) + one).toByte
    stopRow
  }

  /** Scan one column.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    */
  def scan(startRow: ByteArray, stopRow: ByteArray, family: String): RowScanner = {
    scan(startRow, stopRow, family, Seq.empty)
  }

  /** Scan one or more columns. If columns is empty, get all columns in the column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    */
  def scan(startRow: ByteArray, stopRow: ByteArray, family: String, columns: Seq[ByteArray]): RowScanner = {
    scan(startRow, startRow, Seq((family, columns)))
  }

  /** Scan the range for all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    */
  def scan(startRow: ByteArray, stopRow: ByteArray): RowScanner = {
    scan(startRow, stopRow, Seq.empty)
  }

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    */
  def scan(startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner

  /** Scan the whole table. */
  def scan(family: String): RowScanner = {
    scan(TableStartRow, TableEndRow, family)
  }

  /** Scan the whole table. */
  def scan(family: String, columns: Seq[ByteArray]): RowScanner = {
    scan(TableStartRow, TableEndRow, family, columns)
  }

  /** Scan the whole table. */
  def scan(families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    scan(TableStartRow, TableEndRow, families)
  }

  /** Scan the whole table. */
  def scan(): RowScanner = {
    scan(TableStartRow, TableEndRow)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: ByteArray, family: String): RowScanner = {
    scan(prefix, nextRowKeyForPrefix(prefix), family)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: ByteArray, family: String, columns: Seq[ByteArray]): RowScanner = {
    scan(prefix, nextRowKeyForPrefix(prefix), family, columns)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    scan(prefix, nextRowKeyForPrefix(prefix), families)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: ByteArray): RowScanner = {
    scan(prefix, nextRowKeyForPrefix(prefix))
  }
}

/** Intra-row scan iterator */
trait IntraRowScanner extends Iterator[Column] {
  def close: Unit
}

/** If BigTable supports intra-row scan. */
trait IntraRowScan {
  /** Scan a column range for a given row.
    * @param startColumn column to start scanner at or after (inclusive)
    * @param stopColumn column to stop scanner before or at (inclusive)
    */
  def intraRowScan(row: ByteArray, family: String, startColumn: ByteArray, stopColumn: ByteArray): IntraRowScanner
}

object ScanFilter {
  object CompareOperator extends Enumeration {
    type CompareOperator = Value
    val Equal, NotEqual, Greater, GreaterOrEqual, Less, LessOrEqual = Value
  }

  import CompareOperator._
  sealed trait Expression
  case class And(list: Seq[Expression]) extends Expression
  case class Or (list: Seq[Expression]) extends Expression
  /** If the filterIfMissing flag is true, the row will not be emitted if the specified column to check is not found in the row. */
  case class BasicExpression(op: CompareOperator, family: String, column: ByteArray, value: ByteArray, filterIfMissing: Boolean = true) extends Expression
}

/** If BigTable supports filter. */
trait FilterScan extends RowScan {
  /** Scan a column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String): RowScanner = {
    scan(filter, startRow, stopRow, family, Seq.empty)
  }

  /** Scan one or more columns. If columns is empty, get all columns in the column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, family: String, columns: Seq[ByteArray]): RowScanner

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray): RowScanner = {
    scan(filter, startRow, stopRow, Seq.empty)
  }

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: ByteArray, stopRow: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner

  /** Get one or more columns. If columns is empty, get all columns in the column family.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: ByteArray, family: String): Seq[Column] = {
    get(filter, row, family, Seq.empty)
  }

  /** Get one or more columns. If columns is empty, get all columns in the column family.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: ByteArray, family: String, columns: Seq[ByteArray]): Seq[Column]

  /** Get the range for all columns in one or more column families. If families is empty, get all column families.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: ByteArray): Seq[ColumnFamily] = {
    get(filter, row, Seq.empty)
  }

  /** Get the range for all columns in one or more column families. If families is empty, get all column families.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Seq[ColumnFamily]

  /** Get the range for all columns in one or more column families. If families is empty, get all column families.
    */
  def getKeyOnly(row: ByteArray, families: String*): Seq[ColumnFamily]

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, family: String): RowScanner = {
    scan(filter, TableStartRow, TableEndRow, family)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, family: String, columns: Seq[ByteArray]): RowScanner = {
    scan(filter, TableStartRow, TableEndRow, family, columns)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression): RowScanner = {
    scan(filter, TableStartRow, TableEndRow)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    scan(filter, TableStartRow, TableEndRow, families)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scan(filter: ScanFilter.Expression, prefix: ByteArray, family: String): RowScanner = {
    scan(filter, prefix, nextRowKeyForPrefix(prefix), family)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scan(filter: ScanFilter.Expression, prefix: ByteArray, family: String, columns: Seq[ByteArray]): RowScanner = {
    scan(filter, prefix, nextRowKeyForPrefix(prefix), family, columns)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scan(filter: ScanFilter.Expression, prefix: ByteArray): RowScanner = {
    scan(filter, prefix, nextRowKeyForPrefix(prefix))
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scan(filter: ScanFilter.Expression, prefix: ByteArray, families: Seq[(String, Seq[ByteArray])]): RowScanner = {
    scan(filter, prefix, nextRowKeyForPrefix(prefix), families)
  }
}


/** If BigTable supports cell level security. */
trait CellLevelSecurity {
  /** Visibility expression which can be associated with a cell.
    * When it is set with a Mutation, all the cells in that mutation will get associated with this expression.
    */
  def setCellVisibility(expression: String): Unit

  /** Returns the current visibility expression setting. */
  def getCellVisibility: String

  /** Visibility labels associated with a Scan/Get deciding which all labeled data current scan/get can access. */
  def setAuthorizations(labels: String*): Unit

  /** Returns the current authorization labels. */
  def getAuthorizations: Seq[String]
}

/** If BigTable supports rollback to previous version of a cell. */
trait Rollback {
  /** Rollback to the previous version for the given column of a row. */
  def rollback(row: ByteArray, family: String, column: ByteArray): Unit = {
    rollback(row, family, Seq(column))
  }

  /** Rollback to the previous version for the given columns of a row.
    * The parameter columns cannot be empty. */
  def rollback(row: ByteArray, family: String, columns: Seq[ByteArray]): Unit

  /** Rollback the columns of a row. The parameter families can not be empty. */
  def rollback(row: ByteArray, families: Seq[(String, Seq[ByteArray])]): Unit
}

/** If BigTable supports appending to a cell. */
trait Appendable {
  /** Append the value of a column. */
  def append(row: ByteArray, family: String, column: ByteArray, value: ByteArray): Unit
}

/** If BigTable supports counter data type. */
trait Counter {
  /** Get the value of a counter column */
  def getCounter(row: ByteArray, family: String, column: ByteArray): Long

  /** Increase a counter with given value (may be negative for decrease). */
  def addCounter(row: ByteArray, family: String, column: ByteArray, value: Long): Unit

  /** Increase a counter with given value (may be negative for decrease). */
  def addCounter(row: ByteArray, families: Seq[(String, Seq[(ByteArray, Long)])]): Unit
}
