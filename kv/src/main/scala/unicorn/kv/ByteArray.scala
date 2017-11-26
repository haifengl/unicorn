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

package unicorn.kv

/**
 * Pimped byte array.
 *
 * @author Haifeng Li
 */
case class ByteArray(bytes: Array[Byte]) extends Ordered[ByteArray] {
  /** Flip each bit of a byte string */
  def unary_~ = ByteArray(bytes.map { b => (~b).toByte })

  /** Hexadecimal string representation. */
  override def toString = {
    bytes.map("%02X" format _).mkString
  }

  override def compare(that: ByteArray): Int = compareByteArray(bytes, that.bytes)

  override def equals(that: Any): Boolean = {
    if (!that.isInstanceOf[ByteArray]) return false
    compare(that.asInstanceOf[ByteArray]) == 0
  }

  override def hashCode: Int = {
    var hash = 7
    bytes.foreach { i => hash = 31 * hash + i }
    hash
  }
}
