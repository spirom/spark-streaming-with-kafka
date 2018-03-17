package util

import java.io.{IOException, File}
import java.net.InetSocketAddress

import com.typesafe.scalalogging.Logger
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer, ServerCnxnFactory}

/**
 * Start/stop a single Zookeeper instance for use by EmbeddedKafkaServer. Do not create one of these directly.
 * @param port
 */
private[util] class EmbeddedZookeeper(port: Int, tempDirs: TemporaryDirectories) {
  private val LOGGER = Logger[EmbeddedZookeeper]
  private var serverConnectionFactory: Option[ServerCnxnFactory] = None

  /**
   * Start a single instance.
   */
  def start() {
    LOGGER.info(s"starting Zookeeper on $port")

    try {
      val zkMaxConnections = 32
      val zkTickTime = 2000
      val zkServer = new ZooKeeperServer(tempDirs.zkSnapshotDir, tempDirs.zkLogDir, zkTickTime)
      serverConnectionFactory = Some(new NIOServerCnxnFactory())
      serverConnectionFactory.get.configure(new InetSocketAddress("localhost", port), zkMaxConnections)
      serverConnectionFactory.get.startup(zkServer)
    }
    catch {
      case e: InterruptedException => {
        Thread.currentThread.interrupt()
      }
      case e: IOException => {
        throw new RuntimeException("Unable to start ZooKeeper", e)
      }
    }
  }

  /**
   * Stop the instance if running.
   */
  def stop() {
    LOGGER.info(s"shutting down Zookeeper on $port")
    serverConnectionFactory match {
      case Some(f) => {
        f.shutdown
        serverConnectionFactory = None
      }
      case None =>
    }
  }
}
