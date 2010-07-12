package org.apache.camel.component.cassandra

import com.shorrockin.cascal.session.Session
import com.shorrockin.cascal.testing.{EmbeddedTestCassandra, CassandraTestPool}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 */

trait CassandraSuite extends BeforeAndAfterAll with CassandraTestPool {
  this: Suite =>

  def withSession(block: Session => Unit): Unit = {
    borrow {session => block(session)}
  }


  override protected def beforeAll() = {
    EmbeddedTestCassandra.init
  }
}