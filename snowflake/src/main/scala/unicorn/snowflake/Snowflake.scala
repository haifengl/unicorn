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

package unicorn.snowflake

import com.typesafe.scalalogging.Logger

/** 64 bit ID generator based on Twitter Snowflake.
  * An id is composed of:
  *  time - 41 bits (millisecond precision with a custom epoch gives us 69 years)
  *  configured worker id - 10 bits - up to 1024 workers/threads/machines
  *  sequence number - 12 bits - rolls over every 4096 per worker (with protection to avoid rollover in the same ms)
  *
  * You should use NTP to keep your system clock accurate.
  * Snowflake protects from non-monotonic clocks, i.e. clocks
  * that run backwards.  If your clock is running fast and NTP
  * tells it to repeat a few milliseconds, snowflake will refuse
  * to generate ids until a time that is after the last time
  * we generated an id. Even better, run in a mode where NTP
  * won't move the clock backwards.
  *
  * @param worker worker id, which can be given 'by hand' per process, or by ZooKeeper on its startup.
  * @param sequence the start sequence number, increasing in the same ms with protection to avoid rollover
  *
  * @author Haifeng Li
  */
class Snowflake(val worker: Long, var sequence: Long = 0L) {
  private lazy val logger = Logger(getClass)

  import Snowflake.{epoch, workerIdBits, maxWorkerId, sequenceBits}

  private val workerIdShift = sequenceBits
  private val timestampLeftShift = Snowflake.sequenceBits + workerIdBits
  private val sequenceMask = -1L ^ (-1L << sequenceBits)

  private var lastTimestamp = -1L

  // sanity check for worker id
  if (worker > maxWorkerId || worker < 0) {
    throw new IllegalArgumentException(s"worker Id can't be greater than $maxWorkerId or less than 0")
  }

  logger.info(s"worker starting. timestamp left shift $timestampLeftShift, worker id bits $workerIdBits, sequence bits $sequenceBits, worker id $worker")

  def next: Long = {
    var timestamp = System.currentTimeMillis

    if (timestamp < lastTimestamp) {
      logger.error("clock is moving backwards. Rejecting requests until {}.", lastTimestamp)
      throw new IllegalStateException(s"Clock moved backwards. Refusing to generate id for ${lastTimestamp - timestamp} milliseconds")
    }

    if (lastTimestamp == timestamp) {
      sequence = (sequence + 1) & sequenceMask
      if (sequence == 0)
        timestamp = waitNextMillis(lastTimestamp)
    } else {
      sequence = 0
    }

    lastTimestamp = timestamp
    ((timestamp - epoch) << timestampLeftShift) |
      (worker << workerIdShift) |
      sequence
  }

  /** Wait until the next millisecond. */
  private def waitNextMillis(lastTimestamp: Long): Long = {
    var timestamp = System.currentTimeMillis
    while (timestamp <= lastTimestamp) {
      timestamp = System.currentTimeMillis
    }
    timestamp
  }
}

object Snowflake {
  lazy val logger = Logger(getClass)

  val epoch = 1463322997653L
  val workerIdBits = 10L
  val maxWorkerId = -1L ^ (-1L << workerIdBits)
  val sequenceBits = 12L

  /** Creates a Snowflake with worker id coordinated by zookeeper.
    *
    * @param zookeeper ZooKeeper connection string.
    * @param group ZooKeeper group node path.
    * @param sequence The start sequence number.
    * @return A Snowflake worker.
    */
  def apply(zookeeper: String, group: String, sequence: Long = 0L): Snowflake = {

    import java.util.concurrent.CountDownLatch
    import org.apache.zookeeper._, KeeperException.NodeExistsException

    logger.debug("Create Snowflake worker with ZooKeeper {}", zookeeper)

    val connectedSignal = new CountDownLatch(1)
    val zk = new ZooKeeper(zookeeper, 5000, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        if (event.getState == Watcher.Event.KeeperState.SyncConnected) {
          connectedSignal.countDown
        }
      }
    })
    connectedSignal.await

    val path = group.split('/')
    for (i <- 2 to path.length) {
      val parent = path.slice(1, i).mkString("/", "/", "")
      if (zk.exists(parent, false) == null) {
        logger.info("ZooKeeper group {} doesn't exist. Create it.", parent)
        zk.create(parent, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      }
    }

    for (worker <- 0L to maxWorkerId) {
      try {
        zk.create(s"$group/$worker", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        return new Snowflake(worker, sequence)
      } catch {
        case e: NodeExistsException =>
          logger.debug("ZooKeeper node {}/{} already exists.", group, worker)
      }
    }

    throw new RuntimeException(s"Snowflake group $group reaches maximum number of workers")
  }
}