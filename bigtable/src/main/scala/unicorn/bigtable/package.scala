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

package unicorn

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
  * @author Haifeng Li
  */
package object bigtable {

  val utf8 = Charset.forName("UTF-8")

  implicit def boxByteArray(x: Array[Byte]) = new ByteArray(x)
  implicit def unboxByteArray(x: ByteArray) = x.bytes
  implicit def string2Bytes(x: String) = x.getBytes(utf8)
  implicit def stringSeq2ByteArray(x: Seq[String]) = x.map(_.getBytes(utf8))

  /** Helper function convert ByteBuffer to Array[Byte]. */
  implicit def byteBuffer2ArrayByte(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }

  /** Byte array ordering */
  def compareByteArray(x: Array[Byte], y: Array[Byte]): Int = {
    val n = Math.min(x.length, y.length)
    for (i <- 0 until n) {
      val a: Int = x(i) & 0xFF
      val b: Int = y(i) & 0xFF
      if (a != b) return a - b
    }
    x.length - y.length
  }
}
