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

import java.nio.ByteBuffer
import java.nio.charset.Charset

import unicorn.json._
import unicorn.util.utf8

/** Orderable row key.To get the best performance, it's essential to think
  * carefully about the design of your row key as the most efficient Bigtable
  * queries use the row key, a row prefix, or a row range to retrieve the data.
  */
sealed trait RowKey

/** A key made from only one attribute. */
sealed trait SimpleRowKey extends RowKey

case class IntRowKey(key: JsInt) extends SimpleRowKey

case class LongRowKey(key: JsLong) extends SimpleRowKey

case class DoubleRowKey(key: JsDouble) extends SimpleRowKey

case class DateRowKey(key: JsDate) extends SimpleRowKey

case class ObjectIdRowKey(key: JsObjectId) extends SimpleRowKey

case class UuidRowKey(key: JsUUID) extends SimpleRowKey

case class StringRowKey(key: JsString) extends SimpleRowKey

case class BinaryRowKey(key: JsBinary) extends SimpleRowKey

/** A key made from at least two attributes or simple keys,
  * only simple keys exist in a compound key.
  */
case class CompoundRowKey(key: Array[SimpleRowKey]) extends RowKey

object RowKey {
  def apply(key: JsInt): IntRowKey = IntRowKey(key)
  def apply(key: JsLong): LongRowKey = LongRowKey(key)
  def apply(key: JsDouble): DoubleRowKey = DoubleRowKey(key)
  def apply(key: JsDate): DateRowKey = DateRowKey(key)
  def apply(key: JsObjectId): ObjectIdRowKey = ObjectIdRowKey(key)
  def apply(key: JsUUID): UuidRowKey = UuidRowKey(key)
  def apply(key: JsString): StringRowKey = StringRowKey(key)
  def apply(key: JsBinary): BinaryRowKey = BinaryRowKey(key)
  def apply(key: JsArray): CompoundRowKey = CompoundRowKey(key.elements.map(apply(_)).toArray)

  def apply(key: JsValue): SimpleRowKey = key match {
    case key: JsInt => IntRowKey(key)
    case key: JsLong => LongRowKey(key)
    case key: JsDouble => DoubleRowKey(key)
    case key: JsDate => DateRowKey(key)
    case key: JsObjectId => ObjectIdRowKey(key)
    case key: JsUUID => UuidRowKey(key)
    case key: JsString => StringRowKey(key)
    case key: JsBinary => BinaryRowKey(key)
    case _ => throw new IllegalArgumentException("Unsupported simple row key type: " + key.getClass)
  }
}

/** Document serializer. By default, document key size is up to 64KB.
  *
  * @author Haifeng Li
  */
class RowKeySerializer(
  buffer: ByteBuffer = ByteBuffer.allocate(65536),
  val charset: Charset = utf8
) {

}