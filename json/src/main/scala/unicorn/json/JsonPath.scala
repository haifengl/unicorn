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

package unicorn.json

import io.gatling.jsonpath._
import io.gatling.jsonpath.AST._
import scala.util.Try

/**
 * An implementation of JSONPath that follows the semantics described by Stefan Goessner
 * on his <a href="http://goessner.net/articles/JsonPath/">blog</a>.
 *
 * Adopt from https://github.com/josephpconley/play-jsonpath
 *
 * This parser supports all queries except for queries that rely on expressions of the underlying language
 * like $..book[(@.length-1)]. However, there’s usually a ready workaround as you can execute the same query
 * using $..book[-1:].
 *
 * One deviation from JSONPath is to always flatten the results of a recursive query. Using the bookstore example,
 * typically a query of $..book will return an array with one element, the array of books. If there was another book
 * array somewhere in the document, then $..book will return an array with two elements, both arrays of books.
 * However, if you were to query $..book[2] for our example, you would get the second book in the first array,
 * which assumes that the $..book result has been flattened. In order to make recursion easier and the code simpler,
 * we always flatten the result of recursive queries regardless of the context.
 */
case class JsonPath(json: JsValue) {
  val parser = new Parser

  private def error(msg: String) = throw new Exception("Bad JsonPath: " + msg)

  def apply(path: String): JsValue = {
    val tokens = parser.compile(path).getOrElse(error(path))
    parse(tokens, json)
  }

  /**
   * Update the JsValue. Support only child and array slice operators.
   */
  def update(path: String, value: JsValue): JsValue = {
    val tokens = parser.compile(path).getOrElse(error(path))
    update(tokens.tail, json, value)
    json
  }

  private def parse(tokens: List[PathToken], js: JsValue): JsValue = tokens.foldLeft[JsValue](js)( (js, token) => token match {
    case Field(name) => js match {
      case JsObject(fields) => js(name)
      case JsArray(arr) => JsArray(arr.map(_(name)): _*)
      case _ => error(s"Field $token's parent is not JsObject or JsArray")
    }
    case RecursiveField(name) => js match {
      case JsObject(fields) => {
        var children = js \\ name
        if (children.size > 0 && children.elements.head.isInstanceOf[JsArray]) {
          children = children.elements.flatMap(_.asInstanceOf[JsArray].elements)
        }
        children
      }
      case JsArray(arr) => {
        var children = arr.flatMap(_ \\ name)
        if (children.head.isInstanceOf[JsArray]) {
          children = children.flatMap(_.asInstanceOf[JsArray].elements)
        }
        JsArray(children: _*)
      }
      case _ => error(s"Recursive field $token's parent is not JsObject or JsArray")
    }
    case MultiField(names) => js match {
      case JsObject(fields) => JsArray(fields.filter(f => names.contains(f._1)).map(_._2).toArray: _*)
      case _ => error(s"Multiple fields ($token)'s parent is not JsObject")
    }
    case AnyField => js match {
      case JsObject(fields) => JsArray(fields.map(_._2).toArray: _*)
      case JsArray(arr) => js
      case _ => error(s"AnyField's parent is not JsObject or JsArray")
    }
    case RecursiveAnyField => js
    case ArraySlice(start, stop, step) =>
      val arr = js.asInstanceOf[JsArray]
      var sliced = if (start.getOrElse(0) >= 0) arr.elements.drop(start.getOrElse(0)) else arr.elements.takeRight(Math.abs(start.get))
      sliced = if(stop.getOrElse(arr.elements.size) >= 0) sliced.slice(0, stop.getOrElse(arr.elements.size)) else sliced.dropRight(Math.abs(stop.get))

      if (step < 0) sliced = sliced.reverse

      JsArray(sliced.zipWithIndex.filter(_._2 % Math.abs(step) == 0).map(_._1): _*)
    case ArrayRandomAccess(indices) => {
      val arr = js.asInstanceOf[JsArray].elements
      val selectedIndices = indices.map(i => if (i >= 0) i else arr.size + i).toSet.toSeq

      if (selectedIndices.size == 1) arr(selectedIndices.head) else JsArray(selectedIndices.map(arr(_)): _*)
    }
    case ft: FilterToken => JsArray(parseFilterToken(ft, js): _*)
    case _ => js
  })

  private def parseFilterToken(ft: FilterToken, js: JsValue): Seq[JsValue] = ft match {
    case HasFilter(SubQuery(tokens)) =>
      (for {
        arr <- Try(js.asInstanceOf[JsArray]).toOption
        children <- Try(arr.elements.map(_.asInstanceOf[JsObject])).toOption
      } yield {
          tokens.last match {
            case Field(name) => children.filter(_.fields.keySet.contains(name))
            case MultiField(names) => children.filter(_.fields.keySet.intersect(names.toSet) == names.toSet)
            case _ => error(s"Invalid filter token $ft")
          }
        }).getOrElse(error(s"Invalid filter $ft"))
    case ComparisonFilter(op, lhv, rhv) => {
      val arr = js.asInstanceOf[JsArray]
      arr.elements.filter { obj =>
        val left = parseFilterValue(lhv, obj)
        val right = parseFilterValue(rhv, obj)
        op.apply(left, right)
      }
    }
    case BooleanFilter(binOp, lht, rht) => {
      val leftJs = parseFilterToken(lht, js)
      val rightJs = parseFilterToken(rht, js)

      binOp match {
        case OrOperator => leftJs.union(rightJs).toSet.toSeq
        case AndOperator => leftJs.intersect(rightJs)
      }
    }
  }

  private def parseFilterValue(fv: FilterValue, js: JsValue): Any = fv match {
    case SubQuery(tokens) => Try{
      primitive(parse(tokens, js)) match {
        case n:Number => n.doubleValue()
        case a @ _ => a
      }
    }.getOrElse(error(s"Invalid filter value $fv"))
    case dv: FilterDirectValue => dv.value
  }

  private def primitive(js: JsValue): Any = js match {
    case JsBoolean(b) => b
    case JsInt(n) => n
    case JsLong(n) => n
    case JsDouble(n) => n
    case JsString(s) => s
    case JsDate(t) => t
    case JsUUID(uuid) => uuid
    case JsBinary(b) => b
    case _ => throw new Exception("Not a primitive: " + js)
  }

  private def update(tokens: List[PathToken], js: JsValue, value: JsValue): Unit = tokens match {
    case token :: Nil =>
      token match {
        case Field(name) => js match {
          case JsObject(_) => js(name) = value
          case JsArray(arr) => arr.foreach { e => e(name) = value }
          case _ => error(s"Field $token's parent is not JsObject or JsArray")
        }

        case MultiField(names) => js match {
          case JsObject(_) => names.foreach { name => js(name) = value}
          case _ => error(s"Multiple fields ($token)'s parent is not JsObject")
        }

        case ArraySlice(start, stop, step) =>
          if (step == 0) error("ArraySlice with step 0")
          val arr = js.asInstanceOf[JsArray]
          val from = if (start.getOrElse(0) >= 0) start.getOrElse(0) else Math.abs(start.get)
          val to = if(stop.getOrElse(arr.elements.size) >= 0) stop.getOrElse(arr.elements.size) else Math.abs(stop.get)

          if(step < 0)
            (to until from by step).foreach { i => while (i >= arr.size) arr += JsUndefined; arr(i) = value }
          else
            (from until to by step).foreach { i => while (i >= arr.size) arr += JsUndefined; arr(i) = value }

        case ArrayRandomAccess(indices) =>
          val arr = js.asInstanceOf[JsArray]
          indices.foreach { idx => val i = if (idx >= 0) idx else arr.size + idx; while (i >= arr.size) arr += JsUndefined; js(i) = value }

        case _ => error(s"Unsupported token $token")
      }
    case token :: tl =>
      token match {
        case Field(name) => js match {
          case JsObject(_) => if (js(name) == JsUndefined) js(name) = newChild(tl); update(tl, js(name), value)
          case JsArray(arr) => arr.foreach { e => update(tl, e(name), value) }
          case _ => error(s"Field $token's parent is not JsObject or JsArray")
        }

        case MultiField(names) => js match {
          case JsObject(_) => names.foreach { name =>if (js(name) == JsUndefined) js(name) = newChild(tl); update(tl, js(name), value) }
          case _ => error(s"Multiple fields ($token)'s parent is not JsObject")
        }

        case ArraySlice(start, stop, step) =>
          if (step == 0) error("ArraySlice with step 0")
          val arr = js.asInstanceOf[JsArray]
          val from = if (start.getOrElse(0) >= 0) start.getOrElse(0) else Math.abs(start.get)
          val to = if(stop.getOrElse(arr.elements.size) >= 0) stop.getOrElse(arr.elements.size) else Math.abs(stop.get)

          if(step < 0)
            (to until from by step).foreach { i => while (i >= arr.size) arr += newChild(tl); update(tl, arr(i), value) }
          else
            (from until to by step).foreach { i => while (i >= arr.size) arr += newChild(tl); update(tl, arr(i), value) }

        case ArrayRandomAccess(indices) =>
          val arr = js.asInstanceOf[JsArray]
          indices.foreach { idx => val i = if (idx >= 0) idx else arr.size + idx; while (i >= arr.size) arr += newChild(tl); update(tl, arr(i), value) }

        case _ => error(s"Unsupported token $token")
      }
    case Nil => throw new IllegalStateException("empty JsonPath")
  }

  private def newChild(tl: List[PathToken]): JsValue = tl.head match {
    case Field(_) | MultiField(_) => JsObject()

    case ArraySlice(_, _, _) | ArrayRandomAccess(_) => JsArray()

    case _ => error(s"Unsupported token ${tl.head}")
  }
}
