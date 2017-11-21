package unicorn.unibase

import unicorn.bigtable._
import unicorn.json._

/** Document update operators for columnar JSON serialization.
  * Only top level fields of documents can be set/unset.
  *
  * @author Haifeng Li
  */
trait UpdateOps {
  /** Underlying BigTable. */
  val table: BigTable

  /** Returns the row key of a document. */
  val rowkey: RowKey

  /** Document value serializer. */
  val serializer: JsonSerializer

  /** The \$set operator replaces the values of fields.
    *
    * The document key should not be set.
    *
    * If the field does not exist, \$set will add a new field with the specified
    * value, provided that the new field does not violate a type constraint.
    *
    * In MongoDB, \$set will create the embedded documents as needed to fulfill
    * the dotted path to the field. For example, for a \$set {"a.b.c" : "abc"}, MongoDB
    * will create the embedded object "a.b" if it doesn't exist.
    * However, we don't support this behavior because of the performance considerations.
    * We suggest the the alternative syntax {"a.b" : {"c" : "abc"}}, which has the
    * equivalent effect.
    *
    * To set an element of an array by the zero-based index position,
    * concatenate the array name with the dot (.) and zero-based index position.
    *
    * @param key the key of document.
    * @param doc the fields to update.
    */
  def set(key: Key, doc: JsObject): Unit = {
    val columns = doc.fields.map { case (field, value) =>
      Column(field, serializer.serialize(value))
    }.toSeq

    table.put(rowkey(key), DocumentColumnFamily, columns)
  }

  /** The \$unset operator deletes particular fields.
    *
    * The document key _id & _tenant should not be unset.
    *
    * If the field does not exist, then \$unset does nothing (i.e. no operation).
    *
    * When deleting an array element, \$unset replaces the matching element
    * with undefined rather than removing the matching element from the array.
    * This behavior keeps consistent the array size and element positions.
    *
    * Note that we don't really delete the field but set it as JsUndefined
    * so that we keep the history and be able to time travel back. Otherwise,
    * we will lose the history after a major compaction.
    *
    * @param key the key of document.
    * @param doc the fields to delete.
    */
  def unset(key: Key, doc: JsObject): Unit = {
    val columns = doc.fields.map { case (field, _) => string2Bytes(field) }.toSeq
    table.delete(rowkey(key), DocumentColumnFamily, columns)
  }
}
