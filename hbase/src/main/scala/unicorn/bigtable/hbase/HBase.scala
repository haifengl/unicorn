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

package unicorn.bigtable.hbase

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.io.compress.Compression
import unicorn.bigtable._

/** HBase server adapter.
  *
  * @author Haifeng Li
  */
class HBase(config: Configuration) extends BigTableDatabase[HBaseTable] {
  val connection = ConnectionFactory.createConnection(config)
  val admin = connection.getAdmin

  override def close: Unit = connection.close

  override def apply(name: String): HBaseTable = {
    new HBaseTable(this, name)
  }

  override def tables: Set[String] = {
    admin.listTableNames.filter(!_.isSystemTable).map(_.getNameAsString).toSet
  }

  override def create(name: String, props: Properties, families: String*): Unit = {
    if (admin.tableExists(TableName.valueOf(name)))
      throw new IllegalStateException(s"Creates Table $name, which already exists")
    
    val tableDesc = new HTableDescriptor(TableName.valueOf(name))
    val propNames = props.stringPropertyNames.asScala
    propNames.foreach { p => tableDesc.setConfiguration(p, props.getProperty(p))}
    families.foreach { family =>
      val desc = new HColumnDescriptor(family)
      desc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      //desc.setCompressionType(Compression.Algorithm.SNAPPY)
      //desc.setCompactionCompressionType(Compression.Algorithm.SNAPPY)
      propNames.foreach { p => desc.setConfiguration(p, props.getProperty(p))}
      tableDesc.addFamily(desc)
    }
    admin.createTable(tableDesc)
  }

  override def drop(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  /** Truncates a table and preserves the splits */
  override def truncate(name: String): Unit = {
    admin.truncateTable(TableName.valueOf(name), true)
  }

  override def exists(name: String): Boolean = {
    admin.tableExists(TableName.valueOf(name))
  }

  override def compact(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    admin.majorCompact(tableName)
  }
}

object HBase {
  /* Uses hbase-site.xml and in hbase-default.xml that can be found on the CLASSPATH */
  def apply(): HBase = {
    val config = HBaseConfiguration.create
    new HBase(config)
  }

  def apply(config: Configuration): HBase = {
    new HBase(config)
  }
}
