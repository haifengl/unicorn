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
package object kv {

  private[kv] val UTF8 = Charset.forName("UTF-8")

  implicit def boxByteArray(x: Array[Byte]) = new ByteArray(x)
  implicit def unboxByteArray(x: ByteArray) = x.bytes
  implicit def string2Bytes(x: String) = x.getBytes(UTF8)
  implicit def stringSeq2ByteArray(x: Seq[String]) = x.map(_.getBytes(UTF8))
/*
  /** Helper function convert ByteBuffer to Array[Byte]. */
  implicit def byteBuffer2ArrayByte(buffer: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buffer.position)
    buffer.position(0)
    buffer.get(bytes)
    bytes
  }
*/
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

  /** When scanning for a prefix the scan should stop immediately
    * after the the last key that has the specified prefix. This
    * method calculates the closest next key immediately following
    * the given prefix.
    *
    * @param prefix The key prefix.
    * @param lastKey The (logic) last key in the table.
    * @return the closest next key immediately following the given prefix.
    */
  def prefixEndKey(prefix: Array[Byte], lastKey: Array[Byte]): Array[Byte] = {
    val ff : Byte = 0xFF.toByte
    val one: Byte = 1

    // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
    // Search for the place where the trailing 0xFFs start
    val offset = prefix.reverse.indexOf(ff) match {
      case -1 => prefix.length
      case  x => prefix.length - x - 1
    }

    // We got an 0xFFFF... (only FFs) endRow value which is
    // the last possible prefix before the end of the table.
    // So set it to stop at the 'end of the table'
    if (offset == 0) {
      return lastKey
    }

    // Copy the right length of the original
    val endRow = java.util.Arrays.copyOfRange(prefix, 0, offset)
    // And increment the last one
    endRow(endRow.length - 1) = (endRow(endRow.length - 1) + one).toByte
    endRow
  }
}
