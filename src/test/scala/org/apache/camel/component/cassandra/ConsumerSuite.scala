package org.apache.camel.component.cassandra

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import java.lang.String
import collection.immutable.Map
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.Conversions._
import org.apache.camel.scala.dsl.builder.RouteBuilder
import org.apache.camel.component.cassandra.ConsumerSuite._
import org.apache.camel.component.cassandra.CassandraComponent._
import org.apache.camel.impl.{DefaultConsumerTemplate, DefaultCamelContext}
import org.apache.camel.Exchange
import grizzled.slf4j.Logger
import com.shorrockin.cascal.testing.{EmbeddedTestCassandra, CassandraTestPool}
import org.apache.camel.scala.RichExchange
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

/**
 */
@RunWith(classOf[JUnitRunner])
class ConsumerSuite extends FunSuite with CassandraSuite with ShouldMatchers {
  val logger: Logger = Logger(classOf[ConsumerSuite])




  test("Polling against a cassandra uri with keyspace, columnFamily and column returns correct exchanges") {
    withSession {
      session => {
        11 to 19 foreach {
          akey =>
            session.insert("camel-cassandra" \ "stringCols" \ key(akey) \ column(1) \ value(akey, 1))
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamilyColumm, toPollKeyspaceColumnFamilyColumn))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    11 to 19 foreach {
      expectedKey => {
        val exchange: Exchange = template.receive(toPollKeyspaceColumnFamilyColumn, 20000)
        exchange should not be (null)
        val msg = exchange.getIn
        msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
        msg.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
        msg.getHeader(columnHeader, classOf[String]) should be(column(1))
        msg.getHeader(keyHeader, classOf[String]) should be(key(expectedKey))
        msg.getBody(classOf[String]) should be(value(expectedKey, 1))
        exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
        val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
        logger.debug("Current %d".format(current))
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamilyColumn) should be(null)
    //use batch headers to test batching
  }


  test("Polling against a cassandra uri with keyspace, columnFamily and key returns correct exchanges") {
    withSession {
      session => {
        21 to 29 foreach {
          acol =>
            session.insert("camel-cassandra" \ "stringCols" \ key(1) \ column(acol) \ value(1, acol))
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamilyKey, toPollKeyspaceColumnFamilyKey))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    21 to 29 foreach {
      expectedColumn => {
        val exchange: Exchange = template.receive(toPollKeyspaceColumnFamilyKey, 20000)
        exchange should not be (null)
        val msg = exchange.getIn
        msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
        msg.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
        msg.getHeader(columnHeader, classOf[String]) should be(column(expectedColumn))
        msg.getHeader(keyHeader, classOf[String]) should be(key(1))
        msg.getBody(classOf[String]) should be(value(1, expectedColumn))
        exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
        val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
        logger.debug("Current %d".format(current))
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamilyKey) should be(null)
    //use batch headers to test batching
  }

  test("Polling against a cassandra uri with keyspace, columnFamily supercolumn and column returns correct exchanges") {
    withSession {
      session => {
        31 to 39 foreach {
          akey =>
            session.insert("camel-cassandra" \\ "superStringCols" \ key(akey) \ supercolumn(1) \ column(1) \ value(akey, 1))
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamilySuperColumnAndColumn, toPollKeyspaceColumnFamilySuperColumnAndColumn))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    31 to 39 foreach {
      expectedKey => {
        val exchange: Exchange = template.receive(toPollKeyspaceColumnFamilySuperColumnAndColumn, 20000)
        exchange should not be (null)
        val msg = exchange.getIn
        msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
        msg.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
        msg.getHeader(superColumnHeader, classOf[String]) should be(supercolumn(1))
        msg.getHeader(columnHeader, classOf[String]) should be(column(1))
        msg.getHeader(keyHeader, classOf[String]) should be(key(expectedKey))
        msg.getBody(classOf[String]) should be(value(expectedKey, 1))
        exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
        val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
        logger.debug("Current %d".format(current))
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamilySuperColumnAndColumn) should be(null)
    //use batch headers to test batching
  }

  test("Polling against a cassandra uri with keyspace, columnFamily supercolumn and key returns correct exchanges") {
    withSession {
      session => {
        41 to 49 foreach {
          acol =>
            session.insert("camel-cassandra" \\ "superStringCols" \ key(1) \ supercolumn(1) \ column(acol) \ value(1, acol))
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamilySuperColumnAndKey, toPollKeyspaceColumnFamilySuperColumnAndKey))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    41 to 49 foreach {
      expectedCol => {
        val exchange: Exchange = template.receive(toPollKeyspaceColumnFamilySuperColumnAndKey, 20000)
        exchange should not be (null)
        val msg = exchange.getIn
        msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
        msg.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
        msg.getHeader(superColumnHeader, classOf[String]) should be(supercolumn(1))
        msg.getHeader(columnHeader, classOf[String]) should be(column(expectedCol))
        msg.getHeader(keyHeader, classOf[String]) should be(key(1))
        msg.getBody(classOf[String]) should be(value(1, expectedCol))
        exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
        val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
        logger.debug("Current %d".format(current))
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamilySuperColumnAndKey) should be(null)
    //use batch headers to test batching
  }

  test("Polling against a cassandra uri with keyspace, columnFamily and supercolumn returns correct exchanges") {
    withSession {
      session => {
        51 to 53 foreach {
          akey =>
            54 to 56 foreach {
              acol =>
                session.insert("camel-cassandra" \\ "superStringCols" \ key(akey) \ supercolumn(1) \ column(acol) \ value(akey, acol))
            }
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamilySuperColumn, toPollKeyspaceColumnFamilySuperColumn))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    51 to 53 foreach {
      expectedKey => {
        54 to 56 foreach {
          expectedCol =>
            val exchange: Exchange = template.receive(toPollKeyspaceColumnFamilySuperColumn, 20000)
            exchange should not be (null)
            val msg = exchange.getIn
            msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
            msg.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
            msg.getHeader(superColumnHeader, classOf[String]) should be(supercolumn(1))
            msg.getHeader(columnHeader, classOf[String]) should be(column(expectedCol))
            msg.getHeader(keyHeader, classOf[String]) should be(key(expectedKey))
            msg.getBody(classOf[String]) should be(value(expectedKey, expectedCol))
            exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
            val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
            logger.debug("Current %d".format(current))
        }
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamilySuperColumnAndKey) should be(null)
    //use batch headers to test batching
  }

  test("Polling against a cassandra uri with keyspace and columnFamily returns correct exchanges") {
    withSession {
      session => {
        61 to 63 foreach {
          akey =>
            64 to 66 foreach {
              acol =>
                session.insert("camel-cassandra" \ "stringCols" \ key(akey) \ column(acol) \ value(akey, acol))
            }
        }
      }
    }
    var context = new DefaultCamelContext
    context.addRoutes(new ConsumerTestRouteBuilder(fromPollKeyspaceColumnFamily, toPollKeyspaceColumnFamily))
    context.start
    var template = new DefaultConsumerTemplate(context)
    template.start
    61 to 63 foreach {
      expectedKey => {
        64 to 66 foreach {
          expectedCol =>
            val exchange: Exchange = template.receive(toPollKeyspaceColumnFamily, 20000)
            exchange should not be (null)
            val msg = exchange.getIn
            msg.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
            msg.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
            msg.getHeader(columnHeader, classOf[String]) should be(column(expectedCol))
            msg.getHeader(keyHeader, classOf[String]) should be(key(expectedKey))
            msg.getBody(classOf[String]) should be(value(expectedKey, expectedCol))
            exchange.getProperty(Exchange.BATCH_SIZE, classOf[Int]) should be(3)
            val current = exchange.getProperty(Exchange.BATCH_INDEX, classOf[Int])
            logger.debug("Current %d".format(current))
        }
      }
    }

    template.receiveNoWait(toPollKeyspaceColumnFamily) should be(null)
    //use batch headers to test batching
  }





  def key(i: Int) = "key" + i

  def column(i: Int) = "column" + i

  def supercolumn(i: Int) = "supercolumn" + i

  def value(k: Int, col: Int) = key(k) + column(col) + "Value"

}

class ConsumerTestRouteBuilder(val fr: String, val to: String) extends RouteBuilder {
  from(fr) --> to
}


object ConsumerSuite {
  val fromPollKeyspaceColumnFamilyColumm = "cassandra:/camel-cassandra/stringCols/column1?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamilyColumn = "direct:pollKeyspaceColumnFamilyColumn"
  val fromPollKeyspaceColumnFamilyKey = "cassandra:/camel-cassandra/stringCols/~key1?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamilyKey = "direct:pollKeyspaceColumnFamilyKey"
  val fromPollKeyspaceColumnFamilySuperColumnAndColumn = "cassandra:/camel-cassandra/superStringCols/!supercolumn1/column1?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamilySuperColumnAndColumn = "direct:PollKeyspaceColumnFamilySuperColumnAndColumn"
  val fromPollKeyspaceColumnFamilySuperColumnAndKey = "cassandra:/camel-cassandra/superStringCols/~key1/!supercolumn1?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamilySuperColumnAndKey = "direct:PollKeyspaceColumnFamilySuperColumnAndKey"
  val fromPollKeyspaceColumnFamilySuperColumn = "cassandra:/camel-cassandra/superStringCols/!supercolumn1?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamilySuperColumn = "direct:PollKeyspaceColumnFamilySuperColumn"
  val fromPollKeyspaceColumnFamily = "cassandra:/camel-cassandra/stringCols?cassandraPollingMaxMessages=3"
  val toPollKeyspaceColumnFamily = "direct:PollKeyspaceColumnFamily"

}