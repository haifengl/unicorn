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
import unicorn.oid.BsonObjectId

/** Document Key. Primitive key types are: Int, Long, Double, BigDecimal,
  * LocalDate, LocalDateTime, Timestamp, BSON Object ID, UUID, and Strings.
  * Composite key may have an arbitrary number of fields of any primitive type.
  *
  * @author Haifeng Li
  */
sealed trait Key

sealed trait PrimitiveKey extends Key

case class IntKey(key: Int) extends PrimitiveKey
case class LongKey(key: Long) extends PrimitiveKey
case class DoubleKey(key: Double) extends PrimitiveKey
case class BigDecimalKey(key: BigDecimal) extends PrimitiveKey
case class StringKey(key: String) extends PrimitiveKey
case class LocalDateKey(key: LocalDate) extends PrimitiveKey
case class LocalDateTimeKey(key: LocalDateTime) extends PrimitiveKey
case class TimestampKey(key: Timestamp) extends PrimitiveKey
case class BsonObjectIdKey(key: BsonObjectId) extends PrimitiveKey
case class UUIDKey(key: UUID) extends PrimitiveKey

case class CompoundKey(keys: Seq[PrimitiveKey]) extends Key
