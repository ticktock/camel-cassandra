package org.apache.camel.component.cassandra

import java.net.URI
import collection.JavaConversions._
import org.apache.camel.component.cassandra.CassandraComponent._
import org.apache.camel.{Expression, CamelContext, Producer, Processor}
import org.apache.camel.spi.DataFormat
import org.apache.camel.impl.{DefaultPollingEndpoint, DefaultEndpoint}
import com.shorrockin.cascal.session.{Consistency, Session, SessionPool}
import java.io.ByteArrayInputStream
import org.apache.cassandra.thrift.ConsistencyLevel
import grizzled.slf4j.Logger


/**
 */

class CassandraEndpoint(val uri: String, val context: CamelContext, val pool: SessionPool, val consistency: ConsistencyLevel, val options: java.util.Map[java.lang.String, java.lang.String]) extends DefaultPollingEndpoint(uri, context) {
  private val logger: Logger = Logger(classOf[CassandraEndpoint])
  var keyspaceExtractor: Option[Expression] = None
  var columnFamilyExtractor: Option[Expression] = None
  var supercolumnExtractor: Option[Expression] = None
  var columnExtractor: Option[Expression] = None
  var keyExtractor: Option[Expression] = None
  var valueExtractor: Option[Expression] = None
  var keyspace: Option[String] = None
  var columnFamily: Option[String] = None;
  var key: Option[String] = None
  var superColumn: Option[String] = None
  var column: Option[String] = None
  var dataFormat: Option[DataFormat] = None
  var pollingImpl: Option[CassandraPolling] = None
  var pollingStrategy: Option[CassandraPollingStrategy] = None
  var pollingMaxMessagesPerPoll: Int = 1000
  var pollingMaxKeyRange: Int = 1000
  var batchCreator: Option[BatchCreator] = None

  parseUri
  lookupExtractors
  lookupDataFormat
  lookupPollingOptions
  lookupBatchCreator



  private def parseUri(): Unit = {
    val endpointUri = new URI(uri)
    var path = endpointUri.getPath
    var paths = Nil.iterator.asInstanceOf[Iterator[java.lang.String]]
    if (path != null && path != "/") {
      paths = path.substring(1).split("/").iterator
    }
    if (paths.hasNext) keyspace = Some(paths.next)
    if (paths.hasNext) columnFamily = Some(paths.next)
    if (paths.hasNext) {
      var kocosc = paths.next
      if (kocosc.startsWith("~")) {
        key = Some(kocosc.substring(1))
        if (paths.hasNext) {
          var cosc = paths.next
          if (cosc.startsWith("!")) {
            superColumn = Some(cosc.substring(1))
            if (paths.hasNext) {
              column = Some(paths.next)
            }
          } else {
            column = Some(cosc)
          }
        }
      } else if (kocosc.startsWith("!")) {
        superColumn = Some(kocosc.substring(1))
        if (paths.hasNext) {
          column = Some(paths.next)
        }
      } else {
        column = Some(kocosc)
      }
    }
  }

  private def lookupExtractors(): Unit = {
    keyspaceExtractor = lookupOptionBean(keyspaceExtractorOption, classOf[Expression])
    columnFamilyExtractor = lookupOptionBean(columnFamilyExtractorOption, classOf[Expression])
    supercolumnExtractor = lookupOptionBean(superColumnExtractorOption, classOf[Expression])
    columnExtractor = lookupOptionBean(columnExtractorOption, classOf[Expression])
    keyExtractor = lookupOptionBean(keyExtractorOption, classOf[Expression])
    valueExtractor = lookupOptionBean(valueExtractorOption, classOf[Expression])
  }

  private[cassandra] def withSession[E](block: Session => E): E = {
    pool.borrow {session => block(session)}
  }

  def isSingleton = false

  override def createConsumer(processor: Processor) = {
    if (pollingImpl.isDefined && pollingStrategy.isDefined) {
      logger.error("Both a CassandraPolling instance and a CassandraPollingStragegy instance are defined, which is not supported. Pick one or the other")
      throw new IllegalStateException("Both a Polling instance and a PollingStragegy instance are defined, which is not supported. Pick one or the other")
    }
    var pollingOpt: Option[CassandraPolling] = None
    pollingImpl match {
      case Some(impl) => pollingOpt = Some(impl)
      case _ => None
    }
    pollingStrategy match {
      case Some(strategy) => pollingOpt = Some(new StrategyBasedCassandraPolling(strategy))
      case _ => None
    }
    new CassandraConsumer(this, processor, pollingOpt, pollingMaxMessagesPerPoll)
  }

  def createProducer: Producer = {
    new CassandraProducer(this)
  }

  def getThriftConsistency() = consistency

  def unmarshalIfPossible(value: Array[Byte]): Any = {
    dataFormat match {
      case Some(format: DataFormat) => {
        val buffer = new ByteArrayInputStream(value)
        format.unmarshal(createExchange, buffer)
      }
      case None => {
        value
      }
    }
  }


  private def lookupOptionBean[T <: Any](key: String, clazz: Class[T]): Option[T] = {
    var optionBeanName: Option[String] = asMap(options).remove(key);
    var beanName = optionBeanName match {
    //the default bean name is the option name itself
      case None => key
      case Some(name) => name
    }
    val look = getCamelContext.getRegistry.lookup(beanName, clazz)
    if (look == null) None else Some(look)
  }


  private def lookupDataFormat(): Unit = {
    dataFormat = lookupOptionBean(dataFormatOption, classOf[DataFormat])
  }

  private def lookupPollingOptions(): Unit = {
    asMap(options).remove(cassandraPollingMaxMessagesOption) match {
      case Some(max) => pollingMaxMessagesPerPoll = max.toInt
      case _ => None
    }
    asMap(options).remove(cassandraPollingMaxKeyRangeOption) match {
      case Some(max) => pollingMaxKeyRange = max.toInt
      case _ => None
    }
    pollingImpl = lookupOptionBean(cassandraPollingOption, classOf[CassandraPolling])
    pollingStrategy = lookupOptionBean(cassandraPollingStrategyOption, classOf[CassandraPollingStrategy])

  }

  private def lookupBatchCreator():Unit={
    batchCreator = lookupOptionBean(batchCreatorOption, classOf[BatchCreator]) match {
      case None => Some(new DefaultBatchCreator)
      case Some(creator) => Some(creator)
    }
  }

  override def createEndpointUri = uri
}