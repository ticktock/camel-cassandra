package org.apache.camel.component.cassandra;

import org.apache.camel.Exchange;

import java.util.List;

/**
 */
public interface BatchCreator {


    List<Exchange> createBatch(Exchange incoming);


}
