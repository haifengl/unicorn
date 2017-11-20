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

import org.specs2.mutable._

/**
 * @author Haifeng Li
 */
class JsonSerializerSpec extends Specification {

  "The JsonSerializer" should {
    "serialize JsNull" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsNull)) === JsNull
    }
    "serialize JsTrue" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsTrue)) === JsTrue
    }
    "serialize JsFalse" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsFalse)) === JsFalse
    }
    "serialize 0" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsInt.zero)) === JsInt.zero
    }
    "serialize  '1.23'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsDouble(1.23))) === JsDouble(1.23)
    }
    "serialize \"xyz\"" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsString("xyz"))) === JsString("xyz")
    }
    "serialize escapes in a JsString" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsString("\"\\/\b\f\n\r\t"))) === JsString("\"\\/\b\f\n\r\t")
      serializer.deserialize(serializer.serialize(JsString("Länder"))) === JsString("Länder")
    }
    "serialize '1302806349000'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsLong(1302806349000L))) === JsLong(1302806349000L)
    }
    "serialize '2015-08-10'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsDate("2015-08-10"))) === JsDate("2015-08-10")
    }
    "serialize '10:00:00'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsTime("10:00:00"))) === JsTime("10:00:00")
    }
    "serialize '10:00:00.123'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsTime("10:00:00.123"))) === JsTime("10:00:00")
    }
    "serialize '2015-08-10T10:00:00'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsDateTime("2015-08-10T10:00:00"))) === JsDateTime("2015-08-10T10:00:00")
    }
    "serialize '2015-08-10T10:00:00.123'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsDateTime("2015-08-10T10:00:00.123"))) === JsDateTime("2015-08-10T10:00:00")
    }
    "serialize '2015-08-10 10:00:00.123'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsTimestamp("2015-08-10 10:00:00.123"))) === JsTimestamp("2015-08-10 10:00:00.123")
    }
    "serialize 'CA761232-ED42-11CE-BACD-00AA0057B223'" in {
      val serializer = new JsonSerializer
      serializer.deserialize(serializer.serialize(JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223"))) === JsUUID("CA761232-ED42-11CE-BACD-00AA0057B223")
    }
    "serialize test.json" in {
      val serializer = new JsonSerializer
      val jsonSource = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/test.json")).mkString
      val json = JsonParser(jsonSource)
      val bson = serializer.serialize(json)
      serializer.deserialize(bson) === json
    }
  }
}
