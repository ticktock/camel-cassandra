package org.apache.camel.component.cassandra

import org.apache.camel.Exchange
import java.util.{Collections, List}

/**
 */

class DefaultBatchCreator extends BatchCreator{
  def createBatch(incoming: Exchange): List[Exchange] = Collections.singletonList(incoming)
}