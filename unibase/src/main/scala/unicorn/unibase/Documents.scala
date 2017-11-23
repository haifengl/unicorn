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

import unicorn.json.JsObject

/** A collection of documents.
  *
  * @author Haifeng Li
  */
trait Documents {
  /** The name of document collection. */
  val name: String

  /** Gets a document by row key.
    *
    * @param key row key.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Array[Byte]): Option[JsObject]

  /** Gets a document.
    *
    * @param key document key.
    * @return an option of document. None if it doesn't exist.
    */
  def apply(key: Key): Option[JsObject]

  /** Upserts a document. If a document with same key exists, it will overwritten.
    *
    * @param doc the document.
    */
  def upsert(doc: JsObject): Unit

  /** Removes a document.
    *
    * @param key the document key.
    */
  def delete(key: Key): Unit
}
