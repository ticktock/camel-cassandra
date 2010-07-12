package org.apache.camel.component.cassandra

import org.apache.camel.impl.ScheduledPollConsumer
import java.util.concurrent.ScheduledExecutorService
import java.util.Queue
import reflect.BeanProperty
import org.apache.camel.spi.ShutdownAware
import org.apache.camel.{Exchange, BatchConsumer, ShutdownRunningTask, Processor}
import org.apache.camel.util.ObjectHelper
import grizzled.slf4j.Logger

/**
 */

class CassandraConsumer(val endpoint: CassandraEndpoint,
                        val processor: Processor,
                        exec: ScheduledExecutorService,
                        var pollingOpt: Option[CassandraPolling],
                        @BeanProperty var maxMessagesPerPoll: Int) extends ScheduledPollConsumer(endpoint, processor, exec) with BatchConsumerTrait with ShutdownAwareTrait {
  @volatile protected var shutdownRunningTask: ShutdownRunningTask = null
  @volatile protected var pendingExchanges: Int = 0
  @BeanProperty var useIterator: Boolean = true
  private val logger: Logger = Logger(classOf[CassandraConsumer])


  def this(endpoint: CassandraEndpoint, processor: Processor, pollingOpt: Option[CassandraPolling], max: Int) = {
    this (endpoint, processor, endpoint.getCamelContext().getExecutorServiceStrategy().newScheduledThreadPool(CassandraConsumer, endpoint.getEndpointUri(), 1), pollingOpt, max)
  }

  def poll = {
    shutdownRunningTask = null
    pendingExchanges = 0
    val exchanges = endpoint.withSession {
      session => getPollingImpl.poll(session, endpoint, maxMessagesPerPoll)
    }
    processBatch(exchanges.asInstanceOf[Queue[java.lang.Object]])
  }

  def getPendingExchangesSize = pendingExchanges

  def deferShutdown(task: ShutdownRunningTask) = {
    shutdownRunningTask = task
    false
  }

  def isBatchAllowed: Boolean = {
    val answer = isRunAllowed
    if (!answer) {
      false
    }

    if (shutdownRunningTask == null) {
      // we are not shutting down so continue to run
      true
    } else {
      // we are shutting down so only continue if we are configured to complete all tasks
      ShutdownRunningTask.CompleteAllTasks == shutdownRunningTask;
    }

  }

  def processBatch(exchanges: Queue[java.lang.Object]) = {


    var total: Int = exchanges.size
    if (maxMessagesPerPoll > 0 && total > maxMessagesPerPoll) {
      logger.debug("Limiting to maximum messages to poll %d as there was %d messages in this poll.".format(maxMessagesPerPoll, total))
      total = maxMessagesPerPoll
    }



    var index: Int = 1
    while (index <= total && isBatchAllowed) {

      var holder: DataHolder = ObjectHelper.cast(classOf[DataHolder], exchanges.poll)
      var exchange: Exchange = holder.exchange
      exchange.setProperty(Exchange.BATCH_INDEX, index)
      exchange.setProperty(Exchange.BATCH_SIZE, total)
      exchange.setProperty(Exchange.BATCH_COMPLETE, index == total )
      pendingExchanges = total - index 
      logger.debug("Processing exchange: %s".format(exchange))
      getProcessor.process(exchange)
      try {
        endpoint.withSession {
          session =>
            getPollingImpl.onProcessed(session, endpoint, holder)
        }
      }
      catch {
        case e: Exception => {
          handleException(e)
        }
      }

      index += 1
    }
  }


  private def getPollingImpl: CassandraPolling = {
    pollingOpt match {
      case Some(opt) => opt
      case None => {
        val thePolling = new DefaultCassandraPolling(endpoint)
        pollingOpt = Some(thePolling)
        thePolling
      }
    }
  }

}

object CassandraConsumer
trait BatchConsumerTrait extends BatchConsumer
trait ShutdownAwareTrait extends ShutdownAware

