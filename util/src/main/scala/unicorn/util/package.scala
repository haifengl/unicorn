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

package unicorn

import java.nio.charset.Charset
import java.time.format.DateTimeFormatter

/**
 * Utility functions.
 *
 * @author Haifeng Li
 */
package object util {

  val iso8601DateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val iso8601DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]['Z']")

  val utf8 = Charset.forName("UTF-8")

  /** Measure running time of a function/block. */
  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: " + (System.nanoTime - s)/1e6 + " ms")
    ret
  }
}
