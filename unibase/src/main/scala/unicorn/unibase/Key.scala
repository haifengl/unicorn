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

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID
import unicorn.json.ObjectId

/** Document key. Primitive key types are: Int, Long, Double, BigDecimal,
  * LocalDate, LocalDateTime, Timestamp, BSON Object ID, UUID, and Strings.
  * Composite key may have an arbitrary number of fields of any primitive type.
  *
  * Keep document keys as short as is reasonable such that they can still
  * be useful for required data access.
  *
  * Document keys cannot be changed. The only way they can be "changed"
  * in a table is that the document is deleted and then re-inserted.
  * It pays to get the keys right the first time.
  *
  * @author Haifeng Li
  */
sealed trait Key

sealed trait PrimitiveKey extends Key

case class IntKey(key: Int) extends PrimitiveKey
case class LongKey(key: Long) extends PrimitiveKey
case class DoubleKey(key: Double) extends PrimitiveKey
case class DecimalKey(key: BigDecimal) extends PrimitiveKey
case class StringKey(key: String) extends PrimitiveKey
case class DateKey(key: LocalDate) extends PrimitiveKey
case class DateTimeKey(key: LocalDateTime) extends PrimitiveKey
case class TimestampKey(key: Timestamp) extends PrimitiveKey
case class ObjectIdKey(key: ObjectId) extends PrimitiveKey
case class UUIDKey(key: UUID) extends PrimitiveKey

case class CompositeKey(keys: Seq[PrimitiveKey]) extends Key

object CompositeKey {
  def apply(keys: PrimitiveKey*): CompositeKey = CompositeKey(keys)
}