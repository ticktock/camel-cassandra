package org.apache.camel.component.cassandra

import com.shorrockin.cascal.session.Session
import com.shorrockin.cascal.utils.Conversions._
import grizzled.slf4j.Logger

/**
 */

class StrategyBasedCassandraPolling(val strategy: CassandraPollingStrategy) extends CassandraPolling {
  val logger: Logger = Logger(classOf[StrategyBasedCassandraPolling])

  def poll(session: Session, endpoint: CassandraEndpoint, maxMessages: Int) = {
    var keyspace: String = endpoint.keyspace.getOrElse(throw new IllegalStateException("Keyspace must be statically defined for this consumer"))
    var columnfamily: String = endpoint.columnFamily.getOrElse(throw new IllegalStateException("ColumnFamily must be statically defined for this consumer"))
    endpoint.superColumn match {
      case Some(sc) => {
        endpoint.column match {
          case Some(col) => {
            logger.debug("calling strategy.pollKeyspaceColumnFamilySuperColumnAndColumn(client, %s, %s, %s, %s, %d)".format(keyspace, columnfamily, sc, col, maxMessages))
            strategy.pollKeyspaceColumnFamilySuperColumnAndColumn(session.client, keyspace, columnfamily, sc, col, maxMessages)
          }
          case None => {
            endpoint.key match {
              case Some(key) => {
                logger.debug("calling strategy.pollKeyspaceColumnFamilySuperColumnAndKey(client, %s, %s, %s, %s, %d)".format(keyspace, columnfamily, sc, key, maxMessages))
                strategy.pollKeyspaceColumnFamilySuperColumnAndKey(session.client, keyspace, columnfamily, sc, key, maxMessages)
              }
              case None => {
                logger.debug("calling strategy.pollKeyspaceColumnFamilySuperColumn(client, %s, %s, %s, %d)".format(keyspace, columnfamily, sc, maxMessages))
                strategy.pollKeyspaceColumnFamilySuperColumn(session.client, keyspace, columnfamily, sc, maxMessages)
              }
            }
          }
        }
      }
      case None => {
        endpoint.column match {
          case Some(col) => {
            logger.debug("calling strategy.pollKeyspaceColumnFamilyColumn(client, %s, %s, %s, %d)".format(keyspace, columnfamily, col, maxMessages))
            strategy.pollKeyspaceColumnFamilyColumn(session.client, keyspace, columnfamily, col, maxMessages)
          }
          case None => {
            endpoint.key match {
              case Some(key) => {
                logger.debug("calling strategy.pollKeyspaceColumnFamilyAndKey(client, %s, %s, %s, %d)".format(keyspace, columnfamily, key, maxMessages))
                strategy.pollKeyspaceColumnFamilyAndKey(session.client, keyspace, columnfamily, key, maxMessages)
              }
              case None => {
                logger.debug("calling strategy.pollKeyspaceColumnFamily(client, %s, %s, %d)".format(keyspace, columnfamily, maxMessages))
                strategy.pollKeyspaceColumnFamily(session.client, keyspace, columnfamily, maxMessages)
              }
            }
          }
        }
      }
    }

  }

  def onProcessed(session: Session, endpoint: CassandraEndpoint, holder: DataHolder) = {
    session.client.batch_mutate(holder.keyspace, holder.mutations, endpoint.getThriftConsistency)
  }
}