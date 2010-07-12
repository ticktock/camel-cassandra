package org.apache.camel.component.cassandra

import org.apache.camel.impl.DefaultComponent

import java.lang.String
import reflect.BeanProperty
import com.shorrockin.cascal.session._
import org.apache.cassandra.thrift.ConsistencyLevel
import collection.mutable.HashMap
import java.util.Map
import org.apache.camel._
import java.net.URI
import collection.jcl.Conversions._
import grizzled.slf4j.Logger
import collection.jcl.MapWrapper
import org.apache.camel.component.cassandra.CassandraComponent._

class CassandraComponent(context: CamelContext) extends DefaultComponent(context) {
  def this() = {
    this (null)
  }


  val log: Logger = Logger(classOf[CassandraComponent])

  @BeanProperty var cassandraHost: String = "localhost"
  @BeanProperty var cassandraPort: Int = 9160
  @BeanProperty var cassandraTimeout: Int = 3000
  @BeanProperty var consistencyLevel: ConsistencyLevel = ConsistencyLevel.QUORUM

  var poolMap = new HashMap[Tuple4[String, int, int, ConsistencyLevel], SessionPool]


  override def doStart = {

  }

  private def getPool(host: String, port: Int, timeout: Int, consistency: ConsistencyLevel): SessionPool = {
    synchronized(poolMap) {
      poolMap.get((host, port, timeout, consistency)) match {
        case Some(existing) => {
          log.debug("Returning Existing pool for host:%s port:%d timeout:%d consistency:%s".format(host, port, timeout, consistency))
          return existing
        }
        case None => {
          log.debug("Creating pool for host:%s port:%d timeout:%d consistency:%s".format(host, port, timeout, consistency))
          val params = new PoolParams(20, ExhaustionPolicy.Fail, 500L, 6, 2)
          var hosts = Host(host, port, timeout) :: Nil
          val pool = new SessionPool(hosts, params, new Consistency {def thriftValue = consistency})
          val key = (host, port, timeout, consistency)
          poolMap += key -> pool
          return pool
        }
      }
    }
  }


  override def doStop = {
    poolMap.values.foreach {
      pool => pool.close
    }
  }


  override def useIntrospectionOnEndpoint = false


  protected override def createEndpoint(uriStr: String, remaining: String, opts: Map[java.lang.String, java.lang.Object]): Endpoint = {
    if (!(uriStr.startsWith("cassandra:///") || uriStr.startsWith("cassandra://") || uriStr.startsWith("cassadra:/"))) {
      throw new IllegalArgumentException("Endpoint uri must start with cassandra:/ or cassandra:// or cassandra:///")
    }


    val options = convertMap(opts)
    val uri = new URI(uriStr)
    var host = uri.getHost
    if (host == null) host = cassandraHost
    var port = uri.getPort
    if (port == -1) port = cassandraPort

    val timeout = options.removeKey(timeoutOption) match {
      case Some(str) => str.asInstanceOf[String].toInt
      case None => cassandraTimeout
    }
    val consistency = options.removeKey(consistencyOption) match {
      case Some(lvl) => ConsistencyLevel.valueOf(lvl.asInstanceOf[String])
      case None => consistencyLevel
    }
    val pool = getPool(host, port, timeout, consistency)

    return new CassandraEndpoint(uriStr, getCamelContext, pool, consistency, options.asInstanceOf[MapWrapper[String, String]])
  }
}

object CassandraComponent {
  val timeoutOption = "cassandraTimeout"
  val consistencyOption = "cassandraConsistency"
  val keyspaceExtractorOption = "keyspaceExtractor"
  val columnFamilyExtractorOption = "columnFamilyExtractor"
  val superColumnExtractorOption = "superColumnExtractor"
  val columnExtractorOption = "columnExtractor"
  val keyExtractorOption = "keyExtractor"
  val valueExtractorOption = "valueExtractor"
  val dataFormatOption = "cassandraDataFormat"
  val cassandraPollingOption = "cassandraPollingImpl"
  val cassandraPollingStrategyOption = "cassandraPollingStrategy"
  val cassandraPollingMaxMessagesOption = "cassandraPollingMaxMessages"
  val cassandraPollingMaxKeyRangeOption = "cassandraPollingMaxKeyRange"
  val keyspaceHeader = "camel-cassandra-keyspace"
  val columnFamilyHeader = "camel-cassandra-columnFamily"
  val columnHeader = "camel-cassandra-column"
  val superColumnHeader = "camel-cassandra-supercolumn"
  val keyHeader = "camel-cassandra-key"
}


