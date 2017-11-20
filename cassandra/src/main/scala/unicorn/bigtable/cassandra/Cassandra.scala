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

package unicorn.bigtable.cassandra

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.cassandra.thrift.{ConsistencyLevel, KsDef, CfDef}
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import com.typesafe.scalalogging.Logger
import unicorn.bigtable._

/** Cassandra server adapter.
  *
  * @author Haifeng Li
  */
class Cassandra(transport: TFramedTransport) extends Database[CassandraTable] {
  private lazy val logger = Logger(getClass)

  val protocol = new TBinaryProtocol(transport)
  val client = new Client(protocol)

  override def close: Unit = transport.close

  override def apply(name: String): CassandraTable = {
    new CassandraTable(this, name)
  }

  def apply(name: String, consistency: ConsistencyLevel): CassandraTable = {
    new CassandraTable(this, name, consistency)
  }

  override def tables: Set[String] = {
    client.describe_keyspaces.asScala.map(_.getName).toSet
  }

  /** Create a table with default NetworkTopologyStrategy placement strategy. */
  override def createTable(name: String, families: String*): CassandraTable = {
    val props = new Properties
    props.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy")
    props.put("replication_factor", "3")
    createTable(name, props, families: _*)
  }

  override def createTable(name: String, props: Properties, families: String*): CassandraTable = {
    val replicationStrategy = props.getProperty("class")
    val replicationOptions = props.stringPropertyNames.asScala.filter(_ != "class").map { p => (p, props.getProperty(p)) }.toMap.asJava
    val keyspace = new KsDef
    keyspace.setName(name)
    keyspace.setStrategy_class(replicationStrategy)
    keyspace.setStrategy_options(replicationOptions)

    families.foreach { family =>
      val cf = new CfDef
      cf.setName(family)
      cf.setKeyspace(name)
      keyspace.addToCf_defs(cf)
    }
    
    val schemaVersion = client.system_add_keyspace(keyspace)
    logger.info("create table {}: {}", name, schemaVersion)
    apply(name)
  }
  
  override def dropTable(name: String): Unit = {
    client.system_drop_keyspace(name)
  }

  override def truncateTable(name: String): Unit = {
    client.describe_keyspace(name).getCf_defs.asScala.foreach { cf =>
      client.truncate(cf.getName)
    }
  }

  override def tableExists(name: String): Boolean = {
    client.describe_keyspace(name) != null
  }

  /** Cassandra client API doesn't support compaction.
    * This is actually a nop.
    */
  override def compactTable(name: String): Unit = {
    // fail silently
    logger.warn("Cassandra client API doesn't support compaction")
  }
}

object Cassandra {
  def apply(host: String, port: Int): Cassandra = {
    // For ultra-wide row, we set the maxLength to 16MB.
    // Note that we also need to set the server side configuration
    // thrift_framed_transport_size_in_mb in cassandra.yaml
    // In case of ultra-wide row, it is better to use intra row scan.
    val transport = new TFramedTransport(new TSocket(host, port), 16 * 1024 * 1024)
    transport.open

    new Cassandra(transport)
  }
}
