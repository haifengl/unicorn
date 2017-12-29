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

import unicorn.bigtable._
import unicorn.json._

/** Scan operations.
  *
  * @author Haifeng Li
  */
trait FindOps {
  val index: Seq[Index]

  /** Searches the table.
    * @param query The query predict object in MongoDB style. Supported operators
    *              include \$and, \$or, \$eq, \$ne, \$gt, \$gte (or \$ge), \$lt,
    *              \$lte (or \$le), and \$exists.
    * @param fields Top level fields of documents to return. Empty list returns
    *               the whole document.This parameter should not be used with Drawers.
    * @return an iterator of matched document.
    */
  def find(query: JsObject, fields: String*): Iterator[JsObject] = {
    if (query.fields.isEmpty) {
      //return scan(fields)
    }

    Iterator.empty
  }

  /** Returns the scan filter based on the query predicts.
    *
    * @param query query predict object.
    * @return scan filter.
    */
  private def scanFilter(query: JsObject): CompareExpression = {

    val filters = query.fields.map {
      case ("$or", condition) =>
        require(condition.isInstanceOf[JsArray], "$or predict is not an array")

        val filters = condition.asInstanceOf[JsArray].elements.map { e =>
          require(e.isInstanceOf[JsObject], s"or predict element $e is not an object")
          scanFilter(e.asInstanceOf[JsObject])
        }

        require(!filters.isEmpty, "find: empty $or array")

        if (filters.size > 1) Or(filters) else filters(0)

      case ("$and", condition) =>
        require(condition.isInstanceOf[JsArray], "$and predict is not an array")

        val filters = condition.asInstanceOf[JsArray].elements.map { e =>
          require(e.isInstanceOf[JsObject], s"and predict element $e is not an object")
          scanFilter(e.asInstanceOf[JsObject])
        }

        require(!filters.isEmpty, "find: empty $and array")

        if (filters.size > 1) And(filters) else filters(0)

      case (field, condition) => condition match {
        case JsObject(fields) => fields.toSeq match {
          case Seq(("$eq", value)) => CompareOperation(field, Equal, value)
          case Seq(("$ne", value)) => CompareOperation(field, NotEqual, value)
          case Seq(("$gt", value)) => CompareOperation(field, Greater, value)
          case Seq(("$ge", value)) => CompareOperation(field, GreaterOrEqual, value)
          case Seq(("$gte", value)) => CompareOperation(field, GreaterOrEqual, value)
          case Seq(("$lt", value)) => CompareOperation(field, Less, value)
          case Seq(("$le", value)) => CompareOperation(field, LessOrEqual, value)
          case Seq(("$lte", value)) => CompareOperation(field, LessOrEqual, value)
        }
        case _ => CompareOperation(field, Equal, condition)
      }
    }.toSeq

    require(!filters.isEmpty, "find: empty filter object")

    if (filters.size > 1) And(filters) else filters(0)
  }
}

sealed trait CompareOperator
case object Equal extends CompareOperator
case object NotEqual extends CompareOperator
case object Greater extends CompareOperator
case object GreaterOrEqual extends CompareOperator
case object Less extends CompareOperator
case object LessOrEqual extends CompareOperator

sealed trait CompareExpression
case class CompareOperation(field: String, op: CompareOperator, value: JsValue) extends CompareExpression
case class And(expr: Seq[CompareExpression]) extends CompareExpression
case class Or(expr: Seq[CompareExpression]) extends CompareExpression
