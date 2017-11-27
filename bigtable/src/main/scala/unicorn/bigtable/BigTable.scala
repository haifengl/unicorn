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

package unicorn.bigtable

import java.util.Date

/** Key of cell */
case class CellKey(row: Array[Byte], family: String, qualifier: Array[Byte], timestamp: Long)
/** Cell in wide columnar table */
case class Cell(row: Array[Byte], family: String, qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column of a column family */
case class Column(qualifier: Array[Byte], value: Array[Byte], timestamp: Long = 0)
/** A column family */
case class ColumnFamily(family: String, columns: Seq[Column])
/** A row */
case class Row(key: Array[Byte], families: Seq[ColumnFamily])

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
  def apply(row: Array[Byte], family: String, column: Array[Byte]): Option[Array[Byte]] = {
    val seq = get(row, family, Seq(column))
    if (seq.isEmpty) None else Some(seq.head.value)
  }

  /** Update a value. With it, one may use the syntactic sugar
    * ```
    * table(row, family, column) = value
    * ```
    * Use the client machine UTC time for timestamp.
    */
  def update(row: Array[Byte], family: String, column: Array[Byte], value: Array[Byte]): Unit = {
    put(row, family, column, value, System.currentTimeMillis)
  }

  /** Get one or more columns of a column family. If columns is empty, get all columns in the column family. */
  def get(row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Seq[Column]

  /** Get a column family. */
  def get(row: Array[Byte], family: String): Seq[Column] = {
    get(row, family, Seq.empty)
  }

  /** Get all or some columns in one or more column families.
    * If families is empty, get all column families.
    */
  def get(row: Array[Byte], families: Seq[(String, Seq[Array[Byte]])] = Seq.empty): Seq[ColumnFamily]

  /** Get multiple rows for given column family.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[Array[Byte]], family: String): Seq[Row] = {
    getBatch(rows, family, Seq.empty)
  }

  /** Get multiple rows for given columns. If columns is empty, get all columns of the column family.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[Array[Byte]], family: String, columns: Seq[Array[Byte]]): Seq[Row]

  /** Get multiple rows for given column families. If families is empty, get all column families.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def getBatch(rows: Seq[Array[Byte]], families: Seq[(String, Seq[Array[Byte]])] = Seq.empty): Seq[Row]

  /** Upsert a value. */
  def put(row: Array[Byte], family: String, column: Array[Byte], value: Array[Byte], timestamp: Long): Unit

  /** Upsert values. */
  def put(row: Array[Byte], family: String, columns: Seq[Column]): Unit

  /** Upsert values. */
  def put(row: Array[Byte], families: Seq[ColumnFamily]): Unit

  /** Update the values of one or more rows.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def putBatch(rows: Row*): Unit

  /** Delete the column family of a row. */
  def delete(row: Array[Byte], family: String): Unit = {
    delete(row, family, Seq.empty)
  }

  /** Delete the column of a row. */
  def delete(row: Array[Byte], family: String, column: Array[Byte]): Unit = {
    delete(row, family, Seq(column))
  }

  /** Delete the columns of a row. If columns is empty, delete all columns in the family. */
  def delete(row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Unit

  /** Delete the columns of a row. If families is empty, delete the whole row. */
  def delete(row: Array[Byte], families: Seq[(String, Seq[Array[Byte]])] = Seq.empty): Unit

  /** Delete multiple rows.
    * The implementation may or may not optimize the batch operations.
    * In particular, Accumulo does optimize it.
    */
  def deleteBatch(rows: Seq[Array[Byte]]): Unit
}

/** Row scan iterator */
trait RowIterator extends Iterator[Row] with AutoCloseable {
  def close: Unit
}

/** If the key is ordered in the table, we can scan a range of keys. */
trait OrderedBigTable extends BigTable {
  /** Start row in a table. */
  val TableStartRow: Array[Byte]
  /** End row in a table. */
  val TableEndRow: Array[Byte]

  /** Scan a column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param endRow row to stop scanner before (exclusive)
    */
  def scan(startRow: Array[Byte], endRow: Array[Byte], family: String): RowIterator = {
    scan(startRow, endRow, family, Seq.empty)
  }

  /** Scan one or more columns. If columns is empty, get all columns in the column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param endRow row to stop scanner before (exclusive)
    */
  def scan(startRow: Array[Byte], endRow: Array[Byte], family: String, columns: Seq[Array[Byte]]): RowIterator = {
    scan(startRow, endRow, Seq((family, columns)))
  }

  /** Scan the range for all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param endRow row to stop scanner before (exclusive)
    */
  def scan(startRow: Array[Byte], endRow: Array[Byte]): RowIterator = {
    scan(startRow, endRow, Seq.empty)
  }

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param endRow row to stop scanner before (exclusive)
    */
  def scan(startRow: Array[Byte], endRow: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): RowIterator

  /** Scan the whole table. */
  def scan(family: String): RowIterator = {
    scan(TableStartRow, TableEndRow, family)
  }

  /** Scan the whole table. */
  def scan(family: String, columns: Seq[Array[Byte]]): RowIterator = {
    scan(TableStartRow, TableEndRow, family, columns)
  }

  /** Scan the whole table. */
  def scan(families: Seq[(String, Seq[Array[Byte]])]): RowIterator = {
    scan(TableStartRow, TableEndRow, families)
  }

  /** Scan the whole table. */
  def scan(): RowIterator = {
    scan(TableStartRow, TableEndRow)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: Array[Byte], family: String): RowIterator = {
    scan(prefix, prefixEndKey(prefix, TableEndRow), family)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: Array[Byte], family: String, columns: Seq[Array[Byte]]): RowIterator = {
    scan(prefix, prefixEndKey(prefix, TableEndRow), family, columns)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): RowIterator = {
    scan(prefix, prefixEndKey(prefix, TableEndRow), families)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(prefix: Array[Byte]): RowIterator = {
    scan(prefix, prefixEndKey(prefix, TableEndRow))
  }
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
  case class BasicExpression(op: CompareOperator, family: String, column: Array[Byte], value: Array[Byte], filterIfMissing: Boolean = true) extends Expression
}

/** If BigTable supports server side filter, we can reduce network I/O although not
  * server-side disk I/O. */
trait OrderedBigTableWithFilter extends OrderedBigTable {
  /** Scan a column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: Array[Byte], stopRow: Array[Byte], family: String): RowIterator = {
    scan(filter, startRow, stopRow, family, Seq.empty)
  }

  /** Scan one or more columns. If columns is empty, get all columns in the column family.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: Array[Byte], stopRow: Array[Byte], family: String, columns: Seq[Array[Byte]]): RowIterator

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: Array[Byte], stopRow: Array[Byte]): RowIterator = {
    scan(filter, startRow, stopRow, Seq.empty)
  }

  /** Scan the range for all columns in one or more column families. If families is empty, get all column families.
    * @param startRow row to start scanner at or after (inclusive)
    * @param stopRow row to stop scanner before (exclusive)
    * @param filter filter expression
    */
  def scan(filter: ScanFilter.Expression, startRow: Array[Byte], stopRow: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): RowIterator

  /** Get a column family.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: Array[Byte], family: String): Seq[Column] = {
    get(filter, row, family, Seq.empty)
  }

  /** Get one or more columns. If columns is empty, get all columns in the column family.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Seq[Column]

  /** Get the row.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: Array[Byte]): Seq[ColumnFamily] = {
    get(filter, row, Seq.empty)
  }

  /** Get the range for all columns in one or more column families. If families is empty, get all column families.
    * @param filter filter expression
    */
  def get(filter: ScanFilter.Expression, row: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): Seq[ColumnFamily]

  /** Get the column keys only.
    */
  def getKeyOnly(row: Array[Byte], families: String*): Seq[ColumnFamily]

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, family: String): RowIterator = {
    scan(filter, TableStartRow, TableEndRow, family)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, family: String, columns: Seq[Array[Byte]]): RowIterator = {
    scan(filter, TableStartRow, TableEndRow, family, columns)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression): RowIterator = {
    scan(filter, TableStartRow, TableEndRow)
  }

  /** Scan the whole table. */
  def scan(filter: ScanFilter.Expression, families: Seq[(String, Seq[Array[Byte]])]): RowIterator = {
    scan(filter, TableStartRow, TableEndRow, families)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(filter: ScanFilter.Expression, prefix: Array[Byte], family: String): RowIterator = {
    scan(filter, prefix, prefixEndKey(prefix, TableEndRow), family)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(filter: ScanFilter.Expression, prefix: Array[Byte], family: String, columns: Seq[Array[Byte]]): RowIterator = {
    scan(filter, prefix, prefixEndKey(prefix, TableEndRow), family, columns)
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(filter: ScanFilter.Expression, prefix: Array[Byte]): RowIterator = {
    scan(filter, prefix, prefixEndKey(prefix, TableEndRow))
  }

  /** Scan the rows whose key starts with the given prefix. */
  def scanPrefix(filter: ScanFilter.Expression, prefix: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): RowIterator = {
    scan(filter, prefix, prefixEndKey(prefix, TableEndRow), families)
  }
}

/** Get a row at a given time point. */
trait TimeTravel {
  /** Get a column family. */
  def getAsOf(asOfDate: Date, row: Array[Byte], family: String): Seq[Column] = {
    getAsOfDate(asOfDate, row, family, Seq.empty)
  }

  /** Get one or more columns of a column family. If columns is empty, get all columns in the column family. */
  def getAsOfDate(asOfDate: Date, row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Seq[Column]

  /** Get all columns in one or more column families. If families is empty, get all column families. */
  def getAsOfDate(asOfDate: Date, row: Array[Byte], families: Seq[(String, Seq[Array[Byte]])] = Seq.empty): Seq[ColumnFamily]

}

/** Check and put. Put a row only if the given column doesn't exist. */
trait CheckAndPut {
  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: Array[Byte], checkFamily: String, checkColumn: Array[Byte], family: String, column: Column): Boolean = {
    checkAndPut(row, checkFamily, checkColumn, family, Seq(column))
  }

  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: Array[Byte], checkFamily: String, checkColumn: Array[Byte], family: String, columns: Seq[Column]): Boolean

  /** Insert values. Returns true if the new put was executed, false otherwise. */
  def checkAndPut(row: Array[Byte], checkFamily: String, checkColumn: Array[Byte], families: Seq[ColumnFamily]): Boolean
}

/** Intra-row scan iterator */
trait ColumnIterator extends Iterator[Column] with AutoCloseable {
  def close: Unit
}

/** If BigTable supports intra-row scan. */
trait IntraRowScan {
  /** Scan a column range for a given row.
    * @param startColumn column to start scanner at or after (inclusive)
    * @param stopColumn column to stop scanner before or at (inclusive)
    */
  def intraRowScan(row: Array[Byte], family: String, startColumn: Array[Byte], stopColumn: Array[Byte]): ColumnIterator
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
  def rollback(row: Array[Byte], family: String, column: Array[Byte]): Unit = {
    rollback(row, family, Seq(column))
  }

  /** Rollback to the previous version for the given columns of a row.
    * The parameter columns cannot be empty. */
  def rollback(row: Array[Byte], family: String, columns: Seq[Array[Byte]]): Unit

  /** Rollback the columns of a row. The parameter families can not be empty. */
  def rollback(row: Array[Byte], families: Seq[(String, Seq[Array[Byte]])]): Unit
}

/** If BigTable supports appending to a cell. */
trait Appendable {
  /** Append the value of a column. */
  def append(row: Array[Byte], family: String, column: Array[Byte], value: Array[Byte]): Unit
}

/** If BigTable supports counter data type. */
trait Counter {
  /** Get the value of a counter column */
  def getCounter(row: Array[Byte], family: String, column: Array[Byte]): Long

  /** Increase a counter with given value (may be negative for decrease). */
  def addCounter(row: Array[Byte], family: String, column: Array[Byte], value: Long): Unit

  /** Increase a counter with given value (may be negative for decrease). */
  def addCounter(row: Array[Byte], families: Seq[(String, Seq[(Array[Byte], Long)])]): Unit
}
