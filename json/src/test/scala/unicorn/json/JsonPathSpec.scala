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

import org.specs2.mutable._

class JsonPathSpec extends Specification {
  "JsonPath" should {

    "field" in {
      val jp = JsonPath(testJson)
      jp("$.id") === JsInt(1)
      jp("$['id']") === JsInt(1)
    }

    "recursive field" in {
      JsonPath(testJson)("$..id") === JsArray(2, 3, 4, 1)
    }

    "multi fields" in {
      JsonPath(testJson)("$['id', 'name']") === JsArray("Joe", 1)
    }

    "any field" in {
      val jp = JsonPath(testJson)
      jp("$.*") === JsArray(testJson.fields.map(_._2).toArray: _*)
      jp("$.tags.*") === JsArray(tags.map(JsString(_)): _*)
      jp("$['tags'].*") === JsArray(tags.map(JsString(_)): _*)
    }

    "recursive any" in {
      JsonPath(testJson)("$..*") === testJson
    }

    "array slices" in {
      val jp = JsonPath(testJson)
      tags.indices.foreach{ i =>
        jp("$.tags[" + i + ":]") === JsArray(tags.drop(i).map(JsString(_)): _*)
      }
      jp("$.tags[2]") === JsString("father")
      jp("$.tags[0:3:2]") === JsArray(JsString(tags(0)), JsString(tags(2)))
      jp("$.tags[-2:]") === JsArray(tags.takeRight(2).map(JsString(_)): _*)
      jp("$.tags[:-2]") === JsArray(tags.dropRight(2).map(JsString(_)): _*)
    }

    "array random" in {
      val jp = JsonPath(testJson)
      jp("$.tags[0,2]") === JsArray(JsString(tags(0)), JsString(tags(2)))
      jp("$.tags[-1]") === JsString(tags.last)
    }

    "array recursive" in {
      JsonPath(testJson)("$.address[*].city").asInstanceOf[JsArray].size === 3
    }

    "has filter" in {
      JsonPath(testJson)("$.address[?(@.work)]").asInstanceOf[JsArray].size === 1
    }

    "comparison filter" in {
      val jp = JsonPath(testJson)
      jp("$.address[?(@.id < 3)]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.id <= 3)]").asInstanceOf[JsArray].size === 2

      jp("$.address[?(@.id > 2)]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.id >= 2)]").asInstanceOf[JsArray].size === 3

      jp("$.address[?(@.state == 'PA')]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.city == 'Springfield')]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.city != 'Devon')]").asInstanceOf[JsArray].size === 2
    }

    "boolean filter" in {
      val jp = JsonPath(testJson)
      jp("$.address[?(@.id > 1 && @.state != 'PA')]").asInstanceOf[JsArray].size === 1
      jp("$.address[?(@.id < 4 && @.state == 'PA')]").asInstanceOf[JsArray].size === 2
      jp("$.address[?(@.id == 4 || @.state == 'PA')]").asInstanceOf[JsArray].size === 3
      jp("$.address[?(@.id == 4 || @.state == 'NJ')]").asInstanceOf[JsArray].size === 1
    }

    "update field" in {
      val jp = JsonPath(testJson)
      jp("$.id") = 10
      jp("$.id") === JsInt(10)
      jp("$['id']") = 20
      jp("$['id']") === JsInt(20)
    }

    "update multi fields" in {
      val jp = JsonPath(testJson)
      jp("$['id', 'name']") = 30
      jp("$['id', 'name']") === JsArray(30, 30)
    }

    "update array slices" in {
      val jp = JsonPath(testJson)
      jp("$.tags[2]") = "father"
      jp("$.tags[2]") === JsString("father")
      jp("$.tags[0:3:2]") = "coder"
      jp("$.tags") === JsArray("coder", "husband", "coder", "golfer")
      jp("$.tags[-2:]") = "player"
      jp("$.tags") === JsArray("coder", "husband", "player", "player")
      jp("$.tags[:-2]") = "eater"
      jp("$.tags") === JsArray("eater", "eater", "player", "player")
    }

    "update array random" in {
      val jp = JsonPath(testJson)
      jp("$.tags[0,2]") = "coder"
      jp("$.tags") === JsArray("coder", "husband", "coder", "golfer")
      jp("$.tags[-1]") = "player"
      jp("$.tags") === JsArray("coder", "husband", "coder", "player")
    }

    "update field of nonexistent object" in {
      val jp = JsonPath(testJson)
      jp("$.person.id") = 10
      jp("$.person") !== JsUndefined
      jp("$.person.id") === JsInt(10)
    }

    "update multi fields of nonexistent object" in {
      val jp = JsonPath(testJson)
      jp("$['person']['id', 'name']") = 30
      jp("$.person") !== JsUndefined
      jp("$['person']['id', 'name']") === JsArray(30, 30)
    }

    "update array slices of nonexistent object" in {
      val jp = JsonPath(testJson)
      jp("$.person.tags[1:3]") = "father"
      jp("$.person.tags") === JsArray(JsUndefined, "father", "father")
    }

    "update array random of nonexistent object" in {
      val jp = JsonPath(testJson)
      jp("$.person.tags[2]") = "father"
      jp("$.person.tags") === JsArray(JsUndefined, JsUndefined, "father")
    }
  }

  val tags = Seq("programmer", "husband", "father", "golfer")
  def testJson =
    json"""
      |{
      | "id": 1,
      | "name": "Joe",
      | "tags": ["programmer", "husband", "father", "golfer"],
      | "address": [
      | {
      |   "id": 2,
      |   "street": "123 Main St.",
      |   "city": "Springfield",
      |   "state": "PA"
      | },
      | {
      |   "id": 3,
      |   "street": "456 Main St.",
      |   "city": "Devon",
      |   "state": "PA",
      |   "work": true
      | },
      | {
      |   "id": 4,
      |   "street": "789 Main St.",
      |   "city": "Sea Isle City",
      |   "state": "NJ"
      | }
      | ]
      |}
    """
}
