package org.apache.camel.component.cassandra

import com.shorrockin.cascal.session.Session
import org.apache.camel.Exchange
import java.util.Queue
import org.apache.camel.spi.DataFormat
import java.io.ByteArrayInputStream

/**
 */

trait CassandraPolling {
  def poll(session: Session, endpoint: CassandraEndpoint, maxExchanges: Int): Queue[DataHolder]

  def onProcessed(session: Session, endpoint: CassandraEndpoint, holder: DataHolder)
}