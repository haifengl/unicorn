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

package unicorn.unibase.sql

import unicorn.bigtable.OrderedBigTable
import unicorn.json._
import unicorn.unibase._

/** SQL context of a Narwhal instance.
  *
  * @author Haifeng Li
  */
class SQLContext[+T <: OrderedBigTable](db: BigTableUnibase[T]) {

  def sql(query: String): DataFrame = {
    val sql = SQLParser.parse(query)
    require(sql.isDefined, s"Invalid SQL statement: $query")

    val select = sql.get

    if (select.relations.size > 1)
      throw new UnsupportedOperationException("Join is not supported yet")

    val table = select.relations(0) match {
      case Table(name, _) => db.table(name)
      case Subquery(_, _) => throw new UnsupportedOperationException("Sub query is not supported yet")
    }

    val it = table.find(where2Json(select.where), projections2Json(select.projections))

    val columns = projections2ColumnNames(table.name, select.projections)
    val columnIndex = columns.zipWithIndex.toMap

    val rows = it.map { doc =>
      project(doc, select.projections)
    }

    val data = new DataFrame(columns, rows.toIndexedSeq, Some(query))

    val groupBy = select.groupBy match {
      case None =>
        if (isAggregation(select.projections)) {
          val row = Row(aggregate(columnIndex, data.rows, select.projections): _*)
          new DataFrame(columns, Seq(row), data.explain)
        } else data

      case Some(groupBy) =>
        val groups = data.groupBy(groupBy.keys.map(_.toString): _*)
        val rows = groups.map { case (key, group) => Row(aggregate(columnIndex, group, select.projections): _*) }.toSeq
        new DataFrame(data.columnNames, rows, data.explain)
    }

    val orderBy = select.orderBy match {
      case None => groupBy
      case Some(orderBy) => groupBy.orderBy(orderBy.keys.map {
        case (expr, ASC) => (expr.toString, true)
        case (expr, DESC) => (expr.toString, false)
      }: _*)
    }

    val limit = select.limit match {
      case None => orderBy
      case Some(limit) =>
        new DataFrame(orderBy.columnNames, orderBy.take(limit.rows.toInt), orderBy.explain)
    }

    limit
  }

  private def projections2ColumnNames(table: String, projections: Projections): Seq[String] = {
    projections match {
      case _: AllColumns => Seq(table)
      case ExpressionProjections(lst) =>
        lst.map { case (expr, as) =>
          as match {
            case Some (as) => as
            case None => expr.toString
          }
        }
    }
  }

  private def project(doc: JsObject, projections: Projections): Row = {
    projections match {
      case _: AllColumns => Row(doc)
      case ExpressionProjections(lst) =>
        val jsonPath = JsonPath(doc)
        val elements = lst.map {
          case (FieldIdent(_, field), _) => jsonPath(dotpath2JsonPath(field))
          case (CountExpr(FieldIdent(_, field)), _) => jsonPath(dotpath2JsonPath(field))
          case (_: CountAll, _) => JsNull
          case (Sum(FieldIdent(_, field)), _) => jsonPath(dotpath2JsonPath(field))
          case (Avg(FieldIdent(_, field)), _) => jsonPath(dotpath2JsonPath(field))
          case (Min(FieldIdent(_, field)), _) => jsonPath(dotpath2JsonPath(field))
          case (Max(FieldIdent(_, field)), _) => jsonPath(dotpath2JsonPath(field))
          case projection => throw new UnsupportedOperationException(s"Unsupported project: $projection")
        }
        Row(elements: _*)
    }
  }

  private def dotpath2JsonPath(path: String): String = {
    "$." + path.replaceAll("\\.(\\d+)", "[$1]")
  }

  private def projections2Json(projections: Projections): JsObject = {
    projections match {
      case _: AllColumns => JsObject()
      case ExpressionProjections(lst) =>
        val elements = lst.map {
          case (FieldIdent(_, field), _) => field
          case (CountExpr(FieldIdent(_, field)), _) => field
          case (_: CountAll, _) => "count" //$id
          case (Sum(FieldIdent(_, field)), _) => field
          case (Avg(FieldIdent(_, field)), _) => field
          case (Min(FieldIdent(_, field)), _) => field
          case (Max(FieldIdent(_, field)), _) => field
          case projection => throw new UnsupportedOperationException(s"Unsupported project: $projection")
        }
        JsObject(elements.map(_ -> JsInt(1)): _*)
    }
  }

  private def isAggregation(projections: Projections): Boolean = {
    projections match {
      case _: AllColumns => false
      case ExpressionProjections(lst) =>
        lst.exists {
          case (CountExpr(FieldIdent(_, field)), _) => true
          case (_: CountAll, _) => true
          case (Sum(FieldIdent(_, field)), _) => true
          case (Avg(FieldIdent(_, field)), _) => true
          case (Min(FieldIdent(_, field)), _) => true
          case (Max(FieldIdent(_, field)), _) => true
          case projection => false
        }
    }
  }

  private def aggregate(columnIndex: Map[String, Int], data: Seq[Row], projections: Projections): Seq[JsValue] = {
    projections match {
      case _: AllColumns => throw new IllegalArgumentException("select * with group by")
      case ExpressionProjections(lst) =>
        lst.map {
          case (ident @ FieldIdent(_, field), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            data(0)(index)

          case (ident @ CountExpr(FieldIdent(_, field)), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            val count = data.filter { row => !isNull(row(index)) }.size
            JsInt(count)

          case (_: CountAll, _) => JsInt(data.size)

          case (ident @ Sum(FieldIdent(_, field)), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            val value = data.filter { row => !isNull(row(index)) }.map(_(index).asDouble).sum
            JsDouble(value)

          case (ident @ Avg(FieldIdent(_, field)), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            val values = data.filter { row => !isNull(row(index)) }.map(_(index).asDouble)
            if (values.isEmpty) JsDouble(Double.NaN) else JsDouble(values.sum / values.size)

          case (ident @ Min(FieldIdent(_, field)), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            val values = data.filter { row => !isNull(row(index)) }.map(_(index).asDouble)
            if (values.isEmpty) JsDouble(Double.NaN) else JsDouble(values.min)

          case (ident @ Max(FieldIdent(_, field)), as) =>
            val column = as.getOrElse(ident.toString)
            val index = columnIndex(column)
            val values = data.filter { row => !isNull(row(index)) }.map(_(index).asDouble)
            if (values.isEmpty) JsDouble(Double.NaN) else JsDouble(values.max)

          case projection => throw new UnsupportedOperationException(s"Unsupported project: $projection")
        }
    }
  }

  private def isNull(x: JsValue): Boolean = x == JsUndefined || x == JsNull

  private def where2Json(where: Option[Expression]): JsObject = {
    where match {
      case None => JsObject()
      case Some(expr) =>
        val js = JsObject()
        predict(js, expr)
        js
    }
  }

  private def predict(where: JsObject, expr: Expression): Unit = {
    expr match {
      case And(left, right) =>
        predict(where, left)
        predict(where, right)

      case Or(left, right) =>
        val leftObj = JsObject()
        predict(leftObj, left)
        val rightObj = JsObject()
        predict(rightObj, right)
        where("$or") = JsArray(leftObj, rightObj)

      case Equals(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsInt(value)
      case Equals(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsDouble(value)
      case Equals(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsString(value)
      case Equals(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsDate(value)
      case Equals(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsInt(value)
      case Equals(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsDouble(value)
      case Equals(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsString(value)
      case Equals(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsDate(value)

      case NotEquals(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsObject("$ne" -> JsInt(value))
      case NotEquals(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsObject("$ne" -> JsDouble(value))
      case NotEquals(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsObject("$ne" -> JsString(value))
      case NotEquals(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsObject("$ne" -> JsDate(value))
      case NotEquals(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ne" -> JsInt(value))
      case NotEquals(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ne" -> JsDouble(value))
      case NotEquals(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ne" -> JsString(value))
      case NotEquals(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ne" -> JsDate(value))

      case GreaterThan(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsObject("$gt" -> JsInt(value))
      case GreaterThan(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsObject("$gt" -> JsDouble(value))
      case GreaterThan(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsObject("$gt" -> JsString(value))
      case GreaterThan(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsObject("$gt" -> JsDate(value))
      case GreaterThan(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$lt" -> JsInt(value))
      case GreaterThan(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$lt" -> JsDouble(value))
      case GreaterThan(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$lt" -> JsString(value))
      case GreaterThan(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$lt" -> JsDate(value))

      case GreaterOrEqual(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsObject("$ge" -> JsInt(value))
      case GreaterOrEqual(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsObject("$ge" -> JsDouble(value))
      case GreaterOrEqual(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsObject("$ge" -> JsString(value))
      case GreaterOrEqual(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsObject("$ge" -> JsDate(value))
      case GreaterOrEqual(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$le" -> JsInt(value))
      case GreaterOrEqual(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$le" -> JsDouble(value))
      case GreaterOrEqual(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$le" -> JsString(value))
      case GreaterOrEqual(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$le" -> JsDate(value))

      case LessThan(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsObject("$lt" -> JsInt(value))
      case LessThan(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsObject("$lt" -> JsDouble(value))
      case LessThan(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsObject("$lt" -> JsString(value))
      case LessThan(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsObject("$lt" -> JsDate(value))
      case LessThan(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$gt" -> JsInt(value))
      case LessThan(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$gt" -> JsDouble(value))
      case LessThan(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$gt" -> JsString(value))
      case LessThan(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$gt" -> JsDate(value))

      case LessOrEqual(FieldIdent(_, field), IntLiteral(value)) => where(field) = JsObject("$le" -> JsInt(value))
      case LessOrEqual(FieldIdent(_, field), FloatLiteral(value)) => where(field) = JsObject("$le" -> JsDouble(value))
      case LessOrEqual(FieldIdent(_, field), StringLiteral(value)) => where(field) = JsObject("$le" -> JsString(value))
      case LessOrEqual(FieldIdent(_, field), DateLiteral(value)) => where(field) = JsObject("$le" -> JsDate(value))
      case LessOrEqual(IntLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ge" -> JsInt(value))
      case LessOrEqual(FloatLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ge" -> JsDouble(value))
      case LessOrEqual(StringLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ge" -> JsString(value))
      case LessOrEqual(DateLiteral(value), FieldIdent(_, field)) => where(field) = JsObject("$ge" -> JsDate(value))
    }
  }
}
