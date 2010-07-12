package org.apache.camel.component.cassandra

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.camel.CamelContext
import org.apache.camel.impl.DefaultCamelContext
import org.scalatest.matchers.ShouldMatchers
import com.shorrockin.cascal.testing.CassandraTestPool
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class EndpointSuite extends FunSuite with CassandraSuite with ShouldMatchers {
  test("creating an endpoint from a url with host port keyspace columnFamily column") {
    val context = new DefaultCamelContext()
    val component = new CassandraComponent(context)
    var endpoint = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/stringCols?cassandraTimeout=123&cassandraConsistency=QUORUM")
    endpoint.toString should not be null
  }

  test("pools are shared across endpoints on the same cassandra config") {
    val context = new DefaultCamelContext()
    val component = new CassandraComponent(context)
    var endpoint = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/?cassandraTimeout=123&cassandraConsistency=QUORUM")
    var endpoint2 = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/stringCols?cassandraTimeout=123&cassandraConsistency=QUORUM")
    endpoint.asInstanceOf[CassandraEndpoint].pool should be {endpoint2.asInstanceOf[CassandraEndpoint].pool}
  }

  test("pools are not shared across endpoints that have different cassandra config") {
    val context = new DefaultCamelContext()
    val component = new CassandraComponent(context)
    var endpoint = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/?cassandraTimeout=123&cassandraConsistency=QUORUM")
    var endpoint3 = component.createEndpoint("cassandra://localhost2:9160/camel-cassandra/stringCols?cassandraTimeout=123&cassandraConsistency=QUORUM")
    var endpoint4 = component.createEndpoint("cassandra://localhost:9161/camel-cassandra/stringCols?cassandraTimeout=123&cassandraConsistency=QUORUM")
    var endpoint5 = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/stringCols?cassandraTimeout=1234&cassandraConsistency=QUORUM")
    var endpoint6 = component.createEndpoint("cassandra://localhost:9160/camel-cassandra/stringCols?cassandraTimeout=123&cassandraConsistency=ONE")
    endpoint.asInstanceOf[CassandraEndpoint].pool should not be {endpoint3.asInstanceOf[CassandraEndpoint].pool}
    endpoint.asInstanceOf[CassandraEndpoint].pool should not be {endpoint4.asInstanceOf[CassandraEndpoint].pool}
    endpoint.asInstanceOf[CassandraEndpoint].pool should not be {endpoint5.asInstanceOf[CassandraEndpoint].pool}
    endpoint.asInstanceOf[CassandraEndpoint].pool should not be {endpoint6.asInstanceOf[CassandraEndpoint].pool}
    endpoint3.asInstanceOf[CassandraEndpoint].pool should not be {endpoint4.asInstanceOf[CassandraEndpoint].pool}
    endpoint3.asInstanceOf[CassandraEndpoint].pool should not be {endpoint5.asInstanceOf[CassandraEndpoint].pool}
    endpoint3.asInstanceOf[CassandraEndpoint].pool should not be {endpoint6.asInstanceOf[CassandraEndpoint].pool}
    endpoint4.asInstanceOf[CassandraEndpoint].pool should not be {endpoint5.asInstanceOf[CassandraEndpoint].pool}
    endpoint4.asInstanceOf[CassandraEndpoint].pool should not be {endpoint6.asInstanceOf[CassandraEndpoint].pool}
    endpoint5.asInstanceOf[CassandraEndpoint].pool should not be {endpoint6.asInstanceOf[CassandraEndpoint].pool}

  }


}