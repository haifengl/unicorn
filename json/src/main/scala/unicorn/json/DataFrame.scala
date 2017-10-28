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

package unicorn.json

import java.time.{LocalDate, LocalTime, LocalDateTime}
import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import unicorn.util._

/** Data frame of named columns.
  *
  * @param columnNames column names.
  * @param rows data rows.
  * @param explain optional string of operations (e.g. SQL statement) generating this data frame.
  *
  * @author Haifeng Li
  */
class DataFrame(val columnNames: Seq[String], val rows: Seq[Row], val explain: Option[String] = None) extends Traversable[Row] {
  override def toString: String = toString(10)
  override def copyToArray[B >: Row](xs: Array[B], start: Int, len: Int): Unit = rows.copyToArray(xs, start, len)
  override def find(p: (Row) => Boolean): Option[Row] = rows.find(p)
  override def exists(p: (Row) => Boolean): Boolean = rows.exists(p)
  override def forall(p: (Row) => Boolean): Boolean = rows.forall(p)
  override def foreach[U](p: (Row) => U): Unit = rows.foreach(p)
  override def hasDefiniteSize: Boolean = rows.hasDefiniteSize
  override def isEmpty: Boolean = rows.isEmpty
  override def seq: Traversable[Row] = rows.seq
  override def toIterator: Iterator[Row] = rows.toIterator
  override def toStream: Stream[Row] = rows.toStream
  override def size: Int = rows.size
  override def take(n: Int): Seq[Row] = rows.take(n)
  override def head(): Row = rows.head

  override def filter(f: Row => Boolean): Seq[Row] = rows.filter(f)

  def map(f: Row => Row): Seq[Row] = rows.map(f)

  /** Groups rows by given columns. */
  def groupBy(cols: String*): Map[Seq[JsValue], Seq[Row]] = {
    val index = cols.map { col =>
      val i = columnNames.indexOf(col)
      require(i >= 0, s"Column $col doesn't exist")
      i
    }

    rows.groupBy { row =>
      index.map(row(_))
    }
  }

  /** Sorts the rows in ascending by given columns. */
  def sortBy(cols: String*): DataFrame = {
    orderBy(cols.map((_, true)): _*)
  }

  /** Sorts the rows by given columns.
    *
    * @param cols a tuple of column name and ordering (true for ascending and false for descending).
    */
  def orderBy(cols: (String, Boolean)*): DataFrame = {
    val index = cols.map { case (col, asc) =>
      val i = columnNames.indexOf(col)
      require(i >= 0, s"Column $col doesn't exist")
      (i, asc)
    }

    val sorted = rows.sortWith { case (a, b) =>
      index.foldLeft(true) {
        case (false, _) => false
        case (true, (i, asc)) =>
          val x = a(i)
          val y = b(i)
          if (asc)
            JsValueOrdering.compare(x, y) < 0
          else
            JsValueOrdering.compare(x, y) > 0
      }
    }

    new DataFrame(columnNames, sorted, explain)
  }

  /** Compose the string representing rows for output
    * @param numRows Number of rows to show
    * @param truncate Whether truncate long strings and align cells right
    */
  def toString(numRows: Int, truncate: Boolean = true): String = {
    require(numRows > 0, s"Invalid numRows: $numRows")

    val sb = new StringBuilder
    val hasMoreData = size > numRows
    val data = take(numRows)
    val numCols = columnNames.length
    val maxColumnWidth = numCols match {
      case 1 => 78
      case 2 => 38
      case _ => 20
    }

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond maxColumnWidth characters, truncate it with "..."
    val rows: Seq[Seq[String]] = columnNames +: data.map { row =>
      row.elements.map { cell =>
        val str = cell.toString
        if (truncate && str.length > maxColumnWidth) str.substring(0, maxColumnWidth - 3) + "..." else str
      }
    }

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate) {
        leftPad(cell, colWidths(i))
      } else {
        rightPad(cell, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case (cell, i) =>
        if (truncate) {
          leftPad(cell.toString, colWidths(i))
        } else {
          rightPad(cell.toString, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}

object DataFrame {
  def apply(columns: String*) = {
    new DataFrame(columns, new ArrayBuffer[Row]())
  }
}

/** A row in data frame. */
case class Row(elements: IndexedSeq[JsValue]) extends Traversable[JsValue] {
  override def toString(): String = elements.mkString("[", ",", "]")

  override def copyToArray[B >: JsValue](xs: Array[B], start: Int, len: Int): Unit = elements.copyToArray(xs, start, len)
  override def find(p: (JsValue) => Boolean): Option[JsValue] = elements.find(p)
  override def exists(p: (JsValue) => Boolean): Boolean = elements.exists(p)
  override def forall(p: (JsValue) => Boolean): Boolean = elements.forall(p)
  override def foreach[U](p: (JsValue) => U): Unit = elements.foreach(p)
  override def hasDefiniteSize: Boolean = elements.hasDefiniteSize
  override def isEmpty: Boolean = elements.isEmpty
  override def seq: Traversable[JsValue] = elements.seq
  override def toIterator: Iterator[JsValue] = elements.toIterator
  override def toStream: Stream[JsValue] = elements.toStream

  /** Number of elements in the Row. */
  override def size: Int = elements.size

  /** Returns the value at position i. */
  def apply(i: Int): JsValue = elements(i)

  /** Returns the value at position i. */
  def get(i: Int): JsValue = elements(i)

  /** Checks whether the value at position i is null or undefined. */
  def isNullAt(i: Int): Boolean = {
    val v = elements(i)
    v == JsNull || v == JsUndefined
  }

  /** Returns the value at position i as a primitive boolean. */
  def getBoolean(i: Int): Boolean = elements(i).asBoolean

  /** Returns the value at position i as a primitive int. */
  def getInt(i: Int): Int = elements(i).asInt

  /** Returns the value at position i as a primitive long. */
  def getLong(i: Int): Long = elements(i).asLong

  /** Returns the value at position i as a primitive double. */
  def getDouble(i: Int): Double = elements(i).asDouble

  /** Returns the value at position i as a String object. */
  def getString(i: Int): String = elements(i).toString

  /** Returns the value at position i of date type as LocalDate. */
  def getDate(i: Int): LocalDate = elements(i).asDate

  /** Returns the value at position i of date type as LocalTime. */
  def getTime(i: Int): LocalTime = elements(i).asTime

  /** Returns the value at position i of date type as LocalDateTime. */
  def getDateTime(i: Int): LocalDateTime = elements(i).asDateTime

  /** Returns the value at position i of date type as Timestamp. */
  def getTimestamp(i: Int): Timestamp = elements(i).asDateTimestamp

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    elements.exists { v =>
      v == JsNull || v == JsUndefined
    }
  }
}

object Row {
  def apply(values: JsValue*) = new Row(values.toIndexedSeq)
  def fromSeq(seq: Seq[JsValue]) = new Row(seq.toIndexedSeq)
  val empty = new Row(IndexedSeq.empty)
}