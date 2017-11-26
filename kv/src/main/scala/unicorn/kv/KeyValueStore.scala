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

import java.util.Properties

/** A key-value database.
  *
  * @author Haifeng Li
  */
trait KeyValueStore extends AutoCloseable {
  /** Returns a keyspace/table.
    * @param name the name of keyspace.
    */
  def apply(name: String): Keyspace

  /** Creates a keyspace/table.
    * @param name the name of table.
    * @param props table configurations.
    */
  def create(name: String, props: Properties): Unit

  /** Drops a keyspace/table.
    * @param name the name of keyspace.
    */
  def drop(name: String): Unit
}
