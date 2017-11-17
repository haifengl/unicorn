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

package unicorn.bigtable.accumulo

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.accumulo.core.client.admin.NewTableConfiguration
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import unicorn.bigtable._

/** Accumulo server adapter.
  *
  * @author Haifeng Li
  */
class Accumulo(val connector: Connector) extends Database[AccumuloTable] {
  val tableOperations = connector.tableOperations
  override def close: Unit = () // Connector has no close method

  override def apply(name: String): AccumuloTable = {
    new AccumuloTable(this, name)
  }

  override def tables: Set[String] = {
    connector.tableOperations.list.asScala.toSet
  }

  override def createTable(name: String, props: Properties, families: String*): AccumuloTable = {
    if (connector.tableOperations.exists(name))
      throw new IllegalStateException(s"Creates Table $name, which already exists")

    val config = new NewTableConfiguration
    val settings = props.stringPropertyNames.asScala.map { p => (p, props.getProperty(p)) }.toMap
    config.setProperties(settings.asJava)
    connector.tableOperations.create(name, config)

    val localityGroups = families.map { family =>
      val set: java.util.Set[Text] = new java.util.TreeSet[Text]()
      set.add(new Text(family))
      (family, set)
    }.toMap

    tableOperations.setLocalityGroups(name, localityGroups.asJava)
    apply(name)
  }
  
  override def dropTable(name: String): Unit = {
    if (!connector.tableOperations.exists(name))
      throw new IllegalStateException(s"Drop Table $name, which does not exists")

    tableOperations.delete(name)
  }

  override def truncateTable(name: String): Unit = {
    tableOperations.deleteRows(name, null, null)
  }

  override def tableExists(name: String): Boolean = {
    tableOperations.exists(name)
  }

  override def compactTable(name: String): Unit = {
    tableOperations.compact(name, null, null, true, false)
  }
}

object Accumulo {
  def apply(instance: String, zookeeper: String, user: String, password: String): Accumulo = {
    val inst = new ZooKeeperInstance(instance, zookeeper)
    val conn = inst.getConnector(user, new PasswordToken(password))
    new Accumulo(conn)
  }

  /** Create a mock instance that holds all data in memory, and will
    * not retain any data or settings between runs. It presently does
    * not enforce users, logins, permissions, etc.
    * This is for test purpose only.
    */
  def apply(user: String = "root", password: String = ""): Accumulo = {
    val inst = new MockInstance
    val conn = inst.getConnector(user, new PasswordToken(password))
    new Accumulo(conn)
  }
}
