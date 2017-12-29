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

package unicorn.unibase.graph

import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll
import unicorn.bigtable.accumulo.Accumulo
import unicorn.json._
import unicorn.snowflake.Snowflake
import unicorn.unibase._

/**
 * @author Haifeng Li
 */
class GraphSpec extends Specification with BeforeAfterAll {
  // Make sure running examples one by one.
  // Otherwise, test cases on same columns will fail due to concurrency
  sequential
  val bigtable = Accumulo()
  val db = Unibase(bigtable)
  val graphName = "unicorn_unibase_graph_test"

  implicit val snowflake = new Snowflake(0)

  var saturn   = 0L
  var sky      = 1L
  var sea      = 2L
  var jupiter  = 3L
  var neptune  = 4L
  var hercules = 5L
  var alcmene  = 6L
  var pluto    = 7L
  var nemean   = 8L
  var hydra    = 9L
  var cerberus = 10L
  var tartarus = 11L

  override def beforeAll = {
    db.createPropertyGraph(graphName)
    val gods = db.propertyGraph(graphName)

    gods.add(Node(saturn, json"""{"label": "titan", "name": "saturn", "age": 10000}"""))
    gods.add(Node(sky, json"""{"label": "location", "name": "sky"}"""))
    gods.add(Node(sea, json"""{"label": "location", "name": "sea"}"""))
    gods.add(Node(jupiter, json"""{"label": "god", "name": "jupiter", "age": 5000}"""))
    gods.add(Node(neptune, json"""{"label": "god", "name": "neptune", "age": 4500}"""))
    gods.add(Node(hercules, json"""{"label": "demigod", "name": "hercules", "age": 30}"""))
    gods.add(Node(alcmene, json"""{"label": "human", "name": "alcmene", "age": 45}"""))
    gods.add(Node(pluto, json"""{"label": "god", "name": "pluto", "age": 4000}"""))
    gods.add(Node(nemean, json"""{"label": "monster", "name": "nemean"}"""))
    gods.add(Node(hydra, json"""{"label": "monster", "name": "hydra"}"""))
    gods.add(Node(cerberus, json"""{"label": "monster", "name": "cerberus"}"""))
    gods.add(Node(tartarus, json"""{"label": "location", "name": "tartarus"}"""))

    gods.add(Relationship(jupiter, "father", saturn))
    gods.add(Relationship(jupiter, "lives", sky, json"""{"reason": "loves fresh breezes"}"""))
    gods.add(Relationship(jupiter, "brother", neptune))
    gods.add(Relationship(jupiter, "brother", pluto))

    gods.add(Relationship(neptune, "lives", sea, json"""{"reason": "loves waves"}"""))
    gods.add(Relationship(neptune, "brother", jupiter))
    gods.add(Relationship(neptune, "brother", pluto))

    gods.add(Relationship(hercules, "father", jupiter))
    gods.add(Relationship(hercules, "mother", alcmene))
    gods.add(Relationship(hercules, "battled", nemean, json"""{"time": 1, "place": {"latitude": 38.1, "longitude": 23.7}}"""))
    gods.add(Relationship(hercules, "battled", hydra, json"""{"time": 2, "place": {"latitude": 37.7, "longitude": 23.9}}"""))
    gods.add(Relationship(hercules, "battled", cerberus, json"""{"time": 12, "place": {"latitude": 39.0, "longitude": 22.0}}"""))

    gods.add(Relationship(pluto, "brother", jupiter))
    gods.add(Relationship(pluto, "brother", neptune))
    gods.add(Relationship(pluto, "lives", tartarus, json"""{"reason": "no fear of death"}"""))
    gods.add(Relationship(pluto, "pet", cerberus))

    gods.add(Relationship(cerberus, "lives", tartarus))
  }

  override def afterAll = {
    db.drop(graphName)
  }

  "Graph" should {
    "get vertex" in {
      val gods = db.propertyGraph(graphName)
      val vertex = gods(alcmene)
      vertex.id === alcmene
      vertex.properties === json"""{"_id": $alcmene, "label": "human", "name": "alcmene", "age": 45}"""
      vertex.inE("mother") === Seq(Edge(hercules, "mother", alcmene, JsNull))
      vertex.inE.size === 1
      vertex.outE.isEmpty === true
    }
    "update vertex" in {
      val gods = db.propertyGraph(graphName)
      val update = json"""
                          {
                            "_id": $alcmene,
                            "$$set": {
                              "gender": "female"
                            },
                            "$$unset": {
                              "age": 1
                            }
                          }
                       """
      gods.update(update)

      val vertex = gods(alcmene)
      vertex.properties === json"""{"_id": $alcmene, "label": "human", "name": "alcmene", "gender": "female"}"""
    }
    "delete vertex" in {
      val gods = db.propertyGraph(graphName)
      gods.deleteVertex(alcmene)
      gods(alcmene) should throwA[IllegalArgumentException]

      val vertex = gods(hercules)
      vertex.id === hercules
      vertex.outE("mother") should throwA[NoSuchElementException]
    }
    "add string vertex" in {
      val gods = db.propertyGraph(graphName)
      val key = "abc"
      val v = gods.addVertex("abc")
      val vertex = gods(v)
      vertex.id === v
      vertex.properties === json"""{"_id": $v, "_key": "$key"}"""
      vertex.inE.isEmpty === true
      vertex.outE.isEmpty === true
    }
    "delete string vertex" in {
      val gods = db.propertyGraph(graphName)
      val key = "abc"
      gods.deleteVertex(key)

      val v = gods.addVertex("abc")
      gods.deleteVertex(v)

      gods(v) should throwA[IllegalArgumentException]
      gods(key) should throwA[IllegalArgumentException]
    }
    "get edge" in {
      val gods = db.propertyGraph(graphName)
      gods(neptune, "lives", sea) === Some(json"""{"reason": "loves waves"}""")
      gods(neptune, "lives", jupiter) === None
      gods(neptune, "brother", jupiter) === Some(JsNull)
    }
    "delete edge" in {
      val gods = db.propertyGraph(graphName)
      gods.deleteEdge(neptune, "lives", sea)
      gods(neptune, "lives", sea) === None
    }
  }
}
