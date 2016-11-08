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

import java.text.SimpleDateFormat
import java.util.{Date, UUID, TimeZone}
import scala.language.dynamics
import scala.language.implicitConversions
import unicorn.oid.BsonObjectId

/**
 * JSON value.
 *
 * @author Haifeng Li
 */
sealed trait JsValue extends Dynamic {
  override def toString = compactPrint
  def compactPrint = CompactPrinter(this)
  def prettyPrint = PrettyPrinter(this)

  /** Look up field in the current object.
    *
    * @return the field or JsUndefined
    */
  def \(key: String): JsValue = apply(key)
  def \(key: Symbol): JsValue = \(key.name)

  /** Look up field in the current object and all descendants.
    *
    * @return the JsArray of matching nodes
    */
  def \\(key: String): JsArray = JsArray()
  def \\(key: Symbol): JsArray = \\(key.name)

  def apply(key: String): JsValue = {
    throw new UnsupportedOperationException
  }

  def apply(key: Symbol): JsValue = apply(key.name)

  def apply(index: Int): JsValue = {
    throw new UnsupportedOperationException
  }

  def apply(start: Int, end: Int): JsArray = {
    throw new UnsupportedOperationException
  }

  def apply(start: Int, end: Int, step: Int): JsArray = {
    throw new UnsupportedOperationException
  }

  def apply(range: Range): JsArray = {
    throw new UnsupportedOperationException
  }

  def applyDynamic(key: String): JsValue = {
    throw new UnsupportedOperationException
  }

  def selectDynamic(key: String): JsValue = {
    throw new UnsupportedOperationException
  }

  def remove(key: String): Option[JsValue] = {
    throw new UnsupportedOperationException
  }

  def remove(index: Int): JsValue = {
    throw new UnsupportedOperationException
  }

  def update(key: String, value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def update(index: Int, value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def updateDynamic(key: String)(value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def updateDynamic(index: Int)(value: JsValue): JsValue = {
    throw new UnsupportedOperationException
  }

  def get(key: String): Option[JsValue] = {
    throw new UnsupportedOperationException
  }

  def get(key: Symbol): Option[JsValue] = get(key.name)

  def asBoolean: Boolean = {
    throw new UnsupportedOperationException
  }

  def asInt: Int = {
    throw new UnsupportedOperationException
  }

  def asLong: Long = {
    throw new UnsupportedOperationException
  }

  def asDouble: Double = {
    throw new UnsupportedOperationException
  }

  def asDate: Date = {
    throw new UnsupportedOperationException
  }
}

object JsValueOrdering extends Ordering[JsValue] {
  def compare(a: JsValue, b: JsValue) = {
    try {
      a.asDouble.compare(b.asDouble)
    } catch {
      case e: UnsupportedOperationException => a.toString.compare(b.toString)
    }
  }
}

case object JsNull extends JsValue {
  override def toString = "null"
  override def asBoolean: Boolean = false
  override def asInt: Int = 0
  override def asLong: Long = 0L
  override def asDouble: Double = 0.0
}

case object JsUndefined extends JsValue {
  override def toString = "undefined"
  override def asBoolean: Boolean = false
  override def asInt: Int = 0
  override def asLong: Long = 0L
  override def asDouble: Double = 0.0
}

case class JsBoolean(value: Boolean) extends JsValue with Ordered[JsBoolean] {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: Boolean => value == that
    case JsBoolean(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = value
  override def asInt: Int = if (value) 1 else 0
  override def asLong: Long = if (value) 1L else 0L
  override def asDouble: Double = if (value) 1.0 else 0.0

  override def compare(that: JsBoolean): Int = {
    (value, that.value) match {
      case (true, true) => 0
      case (false, false) => 0
      case (true, false) => 1
      case (false, true) => -1
    }
  }
}

object JsBoolean {
  def apply(b: Byte) = if (b != 0) JsTrue else JsFalse
  def apply(b: Int)  = if (b != 0) JsTrue else JsFalse
}

case class JsInt(value: Int) extends JsValue with Ordered[JsInt] {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: Int => value == that
    case that: Short => value == that
    case that: Long => value == that
    case that: Byte => value == that
    case JsInt(that) => value == that
    case JsLong(that) => value == that
    case JsCounter(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = value != 0
  override def asInt: Int = value
  override def asLong: Long = value
  override def asDouble: Double = value

  override def compare(that: JsInt): Int = {
    value - that.value
  }
}

object JsInt {
  val zero = JsInt(0)
}

case class JsLong(value: Long) extends JsValue with Ordered[JsLong] {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: Int => value == that
    case that: Short => value == that
    case that: Long => value == that
    case that: Byte => value == that
    case JsInt(that) => value == that
    case JsLong(that) => value == that
    case JsCounter(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = value != 0
  override def asInt: Int = value.toInt
  override def asLong: Long = value
  override def asDouble: Double = value

  override def compare(that: JsLong): Int = {
    value.compare(that.value)
  }
}

object JsLong {
  val zero = JsLong(0L)
}

/** A counter is a 64 bit integer. The difference from JsLong
  * is mostly for internal representation in database. For encoding
  * reason, the effective number of bits are 56, which should be
  * big enough in practice.
  */
case class JsCounter(value: Long) extends JsValue with Ordered[JsCounter] {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: Int => value == that
    case that: Short => value == that
    case that: Long => value == that
    case that: Byte => value == that
    case JsInt(that) => value == that
    case JsLong(that) => value == that
    case JsCounter(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = value != 0
  override def asInt: Int = value.toInt
  override def asLong: Long = value
  override def asDouble: Double = value

  override def compare(that: JsCounter): Int = {
    value.compare(that.value)
  }
}

object JsCounter {
  val zero = JsCounter(0L)
}

case class JsDouble(value: Double) extends JsValue with Ordered[JsDouble] {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: Double => value == that
    case JsDouble(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = Math.abs(value) > 2*Double.MinValue
  override def asInt: Int = value.toInt
  override def asLong: Long = value.toLong
  override def asDouble: Double = value

  override def compare(that: JsDouble): Int = {
    value.compare(that.value)
  }
}

object JsDouble {
  val zero = JsDouble(0.0)
}

case class JsString(value: String) extends JsValue with Ordered[JsString] {
  override def toString = value
  override def equals(o: Any) = o match {
    case that: String => value == that
    case JsString(that) => value == that
    case _ => false
  }
  override def asBoolean: Boolean = !value.isEmpty
  override def asInt: Int = Integer.parseInt(value)
  override def asLong: Long = java.lang.Long.parseLong(value)
  override def asDouble: Double = java.lang.Double.parseDouble(value)
  override def asDate: Date = JsDate.format.parse(value)

  override def compare(that: JsString): Int = {
    value.compare(that.value)
  }
}

object JsString {
  val empty = JsString("")
}

/** Date times in UTC times. The date time should be in format
  * yyyy-MM-dd'T'HH:mm:ss.SSS'Z' such as 2012-07-20T08:00:00.000Z.
  */
case class JsDate(value: Date) extends JsValue with Ordered[JsDate] {
  override def toString = JsDate.format.format(value)
  override def equals(o: Any) = o match {
    case that: Date => value == that
    case JsDate(that) => value == that
    case _ => false
  }
  override def asDate: Date = value
  override def asLong: Long = value.getTime
  override def asDouble: Double = value.getTime

  override def compare(that: JsDate): Int = {
    value.compare(that.value)
  }
}

object JsDate {
  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  format.setTimeZone(TimeZone.getTimeZone("UTC"))
  val formatLength = "yyyy-MM-ddTHH:mm:ss.SSSZ".length
  def apply(date: Long) = new JsDate(new Date(date))
  def apply(date: String) = new JsDate(format.parse(date))
}

case class JsUUID(value: UUID) extends JsValue {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: UUID => value == that
    case JsUUID(that) => value == that
    case _ => false
  }
}

object JsUUID {
  val regex = """[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""".r
  val formatLength = 36
  def apply() = new JsUUID(UUID.randomUUID)
  def apply(mostSigBits: Long, leastSigBits: Long) = new JsUUID(new UUID(mostSigBits, leastSigBits))
  def apply(uuid: String) = new JsUUID(UUID.fromString(uuid))
  def apply(uuid: Array[Byte]) = new JsUUID(UUID.nameUUIDFromBytes(uuid))
}

case class JsObjectId(value: BsonObjectId) extends JsValue {
  override def toString = value.toString
  override def equals(o: Any) = o match {
    case that: BsonObjectId => value == that
    case JsObjectId(that) => value == that
    case _ => false
  }
}

object JsObjectId {
  val regex = """ObjectId\([0-9a-fA-F]{24}\)""".r
  val formatLength = 34
  def apply() = new JsObjectId(BsonObjectId.generate)
  def apply(id: String) = new JsObjectId(BsonObjectId(id))
  def apply(id: Array[Byte]) = new JsObjectId(BsonObjectId(id))
}

case class JsBinary(value: Array[Byte]) extends JsValue {
  override def toString = value.map("%02X" format _).mkString
  override def equals(o: Any) = o match {
    case that: Array[Byte] => value.sameElements(that)
    case JsBinary(that) => value.sameElements(that)
    case _ => false
  }
}

case class JsObject(fields: collection.mutable.Map[String, JsValue]) extends JsValue {
  override def \\(key: String): JsArray = {
    fields.foldLeft(collection.mutable.ArrayBuffer[JsValue]())((o, pair) => pair match {
      case (field, value) if key == field => o += value; o++= (value \\ key).elements
      case (_, value) => o ++ (value \\ key)
    })
  }

  override def apply(key: String): JsValue = {
    if (fields.contains(key))
      fields(key)
    else
      JsUndefined
  }

  override def applyDynamic(key: String): JsValue = apply(key)

  override def selectDynamic(key: String): JsValue = apply(key)

  override def remove(key: String): Option[JsValue] = fields.remove(key)

  override def update(key: String, value: JsValue): JsValue = {
    fields(key) = value
    value
  }

  override def updateDynamic(key: String)(value: JsValue): JsValue = update(key, value)

  override def get(key: String): Option[JsValue] = {
    if (fields.contains(key))
      Some(fields(key))
    else
      None
  }
}

object JsObject {
  def apply(fields: (String, JsValue)*) = new JsObject(collection.mutable.Map(fields: _*))
  def apply(map: Map[String, JsValue]) = new JsObject(collection.mutable.Map() ++ map)
}

case class JsArray(elements: collection.mutable.ArrayBuffer[JsValue]) extends JsValue with Traversable[JsValue] {
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

  override def size: Int = elements.size

  override def equals(o: Any) = o match {
    case that: Array[JsValue] => elements.sameElements(that)
    case JsArray(that) => elements.sameElements(that)
    case _ => false
  }

  override def \(key: String): JsArray = {
    JsArray(elements.map(_ \ key): _*)
  }

  override def \\(key: String): JsArray = {
    JsArray(elements.flatMap(_ \\ key): _*)
  }

  override def apply(index: Int): JsValue = {
    val i = if (index >= 0) index else elements.size + index
    elements(i)
  }

  override def apply(start: Int, end: Int): JsArray = {
    apply(start until end)
  }

  override def apply(start: Int, end: Int, step: Int): JsArray = {
    apply(start until end by step)
  }

  override def apply(range: Range): JsArray = {
    JsArray(range.map(elements(_)))
  }

  override def remove(index: Int): JsValue = {
    val i = if (index >= 0) index else elements.size + index
    elements.remove(i)
  }

  override def update(index: Int, value: JsValue): JsValue = {
    val i = if (index >= 0) index else elements.size + index
    elements(i) = value
    value
  }

  override def updateDynamic(index: Int)(value: JsValue): JsValue = update(index, value)

  /** Appends a single element to this array and returns
   * the identity of the array. It takes constant amortized time.
   *
   * @param elem  the element to append.
   * @return      the updated array.
   */
  def +=(elem: JsValue): JsArray = {
    elements += elem
    this
  }

  /** Appends a number of elements provided by a traversable object.
   *  The identity of the array is returned.
   *
   *  @param xs    the traversable object.
   *  @return      the updated buffer.
   */
  def ++=(xs: TraversableOnce[JsValue]): JsValue = {
    elements ++= xs
    this
  }

  /** Prepends a single element to this buffer and returns
   *  the identity of the array. It takes time linear in
   *  the buffer size.
   *
   *  @param elem  the element to prepend.
   *  @return      the updated array.
   */
  def +=:(elem: JsValue): JsValue = {
    elem +=: elements
    this
  }

  /** Prepends a number of elements provided by a traversable object.
   *  The identity of the array is returned.
   *
   *  @param xs    the traversable object.
   *  @return      the updated array.
   */
  def ++=:(xs: TraversableOnce[JsValue]): JsValue = {
    xs ++=: elements
    this
  }

  /** Inserts new elements at the index `n`. Opposed to method
   *  `update`, this method will not replace an element with a new
   *  one. Instead, it will insert a new element at index `n`.
   *
   *  @param idx   the index where a new element will be inserted.
   *  @param seq   the traversable object providing all elements to insert.
   */
  def insertAll(idx: Int, seq: Traversable[JsValue]) {
    elements.insertAll(idx, seq)
  }

  /** Removes the element on a given index position. It takes time linear in
   *  the buffer size.
   *
   *  @param idx     the index which refers to the first element to delete.
   *  @param count   the number of elements to delete
   */
  def remove(idx: Int, count: Int) {
    elements.remove(idx, count)
  }
}

object JsArray {
  def apply(elements: JsValue*) = new JsArray(collection.mutable.ArrayBuffer(elements: _*))
}
