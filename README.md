#camel-cassandra An Apache Camel component for integrating with Apache Cassandra

##Cassandra Component
--------------------

The Cassandra component allows you to store and retrieve data to/from Apache Cassandra.

Maven users will need to add the following dependency to their pom.xml for this component:

        <dependency>
            <groupId>org.apache.camel</groupId>
            <artifactId>camel-cassandra</artifactId>
            <version>x.x.x</version>
            <!-- use the same version as your Camel core version -->
        </dependency>


##Sending to the endpoint
-------------------------

You can store data in cassandra by sending a message to a Cassandra producer endpoint. The destination
keyspace, column family, key, supercolumn (if any), and column to store data in can be determined either statically or dynamically,
based upon the endpoint URI used to configure the producer.


When keyspace, column family, key, supercolumn and column are not statically configured via the URI,
user supplied (or default) Camel Expressions are used to extract the appropriate values from the Exchange.

The default extractors use a Camel Header expression to extract the headers defined below. The default
value to store in Cassandra is the body of the message.

##Receiving from the endpoint
-----------------------------

The CassandraConsumer is a scheduled polling endpoint and will return data based upon the URI used to configure it.
For instance, if keyspace, columnfamily, and key are specified, the consumer will send a message for every column
for that key in the columnfamily, also deleting the column after sending.

If only a keyspace and columnfamily are specified, the Consumer will do a "key-first" polling of the columnfamily
returning and deleting each column for a given key before moving on to the next key.

Most sensible permutations are handled by default. To customize the Polling behavior, you can either specify an
instance of org.apache.camel.component.cassandra.CassandraPollingStrategy, which will be executed by an instance of
org.apache.camel.component.cassandra.StrategyBasedCassandraPolling, or to completely customize polling,
you can specify an instance of org.apache.camel.component.cassandra.CassandraPolling.


### Headers
-----------

By default, the CassandraProducer extracts information unspecified in the url from the following headers, and
also by default, the CassandraConsumer populates the following headers in messages it sends.

<table>
    <tr><th>Header</th><th>Type</th><th>Description</th></tr>
    <tr><td>camel-cassandra-keyspace</td><td>String</td><td>The keyspace in which to store the data</td></tr>
    <tr><td>camel-cassandra-columnFamily</td><td>String</td><td>The column family in which to store the data</td></tr>
    <tr><td>camel-cassandra-supercolumn</td><td>String</td><td>The supercolumn in which to store the data</td></tr>
    <tr><td>camel-cassandra-column</td><td>String</td><td>The column in which to store the data</td></tr>
    <tr><td>camel-cassandra-key</td><td>String</td><td>The key with which to store the data</td></tr>
</table>

The out message of the exchange will have all these headers set to the values used to store the data, as well as the value of
returned by the valueExtractor Expression, set as the body.

The default extractors can be overridden by specifying an Expression to use in the Camel Registry (Spring,JNDI, etc..)
You can simply register a bean of the correct name, or override the name of the bean to use via options in the URI.

##URI format
----------

        cassandra:[//cassandraHost:cassandraPort][/keyspace][/columnFamily][/~key][/!supercolumn][/column]


To successfully store data, all of the values except for supercolumn must be specified or successfully extracted.

If a supercolumn name specified or extracted, then a column in a supercolumn is inserted, otherwise a standard column is inserted.


Here are some example URIs, which would apply default extractors to get the necessary information.

        cassandra://host:port
        (Producer: extract keyspace, columnfamily, supercolumn, column, key, value via default expressions)
        (Consumer: Not Valid)

        cassandra:/keyspace
        (Producer: use host/port configured in the component and extract columnfamily, supercolumn, column,
                key, value via default expressions)
        (Consumer: Not Valid)

        cassandra://host:port/keyspace
        (Producer: extract columnfamily, supercolumn, column, key, value via default expressions)
        (Consumer: Not Valid)

        cassandra:/keyspace/columnFamily
        (Producer: use host/port configured in the component and extract supercolumn, column, key,
                value via default expressions)
        (Consumer: use host/port configured in the component and do a "key-first" consumption of all
                columns for each key in the C.F.)

        cassandra://host:port/keyspace/columnFamily
        (Producer: extract supercolumn, column, key, value via default expressions)
        (Consumer: do a "key-first" consumption of all columns for each key in the C.F.)


        cassandra:/keyspace/columnFamily/column
        (Producer: use host/port configured in the component and extract columnfamily, supercolumn, column,
                key, value via default expressions)
        (Consumer: for each key in the C.F, consume and delete the specified column)

        cassandra:/keyspace/columnFamily/~key
        (Producer: extract column, value via default expressions)
        (Consumer: consume and delete each column in this key in the C.F)


        cassandra:/keyspace/columnFamily/!supercolumn
        (Producer: and extract column, key, value via default expressions)
        (Consumer: "key-first" consumption of each column of each supercolumn of each key in the C.F.)

        cassandra://host:port/keyspace/columnFamily/~key/!supercolumn
        (Producer: extract column, key, value via default expressions)
        (Consumer: consume and delete each column in this key in the supercolumn in the C.F)





##Options
---------

<table>
    <tr><th>Name</th><th>Default Value</th><th>Description</th></tr>
    <tr><td>keyspaceExtractor</td><td>keyspaceExtractor</td><td>The the name of the bean used to lookup the keyspace extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>columnFamilyExtractor</td><td>columnFamilyExtractor</td><td>The the name of the bean used to lookup the columnFamily extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>superColumnExtractor</td><td>superColumnExtractor</td><td>The the name of the bean used to lookup the superColumn extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>columnExtractor</td><td>columnExtractor</td><td>The the name of the bean used to lookup the column extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>keyExtractor</td><td>keyExtractor</td><td>The the name of the bean used to lookup the key extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>valueExtractor</td><td>valueExtractor</td><td>The the name of the bean used to lookup the value extractor Expression from the registry. If none is found, the default Expression is used</td></tr>
    <tr><td>cassandraDataFormat</td><td>cassandraDataFormat</td><td>The the name of the bean used to lookup the optional Camel Data Format to use. If found the Data Format is used to marshal the value returned by the value extractor expression before storing, and is used to unmarshall the data in cassandra when reading</td></tr>
    <tr><td>cassandraTimeout</td><td>3000</td><td>Timeout on calls to Cassandra</td></tr>
    <tr><td>cassandraConsistency</td><td>QUORUM</td><td>The Consistency Level to use when storing/loading data to/from cassandra</td></tr>
    <tr><td>cassandraPollingImpl</td><td>instance of org.apache.camel.component.cassandra.DefaultCassandraPolling</td><td>instance of CassandraPolling to use to poll cassandra</td></tr>
    <tr><td>cassandraPollingStrategy</td><td>None</td><td>If you specify an instance of CassndraPollingStrategy, it will be executes with StrategyBasedCassandraPolling</td></tr>
    <tr><td>cassandraPollingMaxMessages</td><td>1000</td><td>The maximum mesages to return per poll</td></tr>
    <tr><td>cassandraPollingMaxKeyRange</td><td>1000</td><td>The maximum mesages to specify in the KeyRange used to call get_range_slices during polling. See Note below.</td></tr>
</table>

###A Note about the Default Polling implementation (before Cassandra 0.7)

Care must be taken when configuring consumer endpoints when no key is specified. When no key is specified, the default polling
implementation calls get_range_slices, using a key range with a empty start and end key. If there are more deleted, uncompacted keys
in the columnFamily than the maximum keys specified in the key range, no data will be returned, even though there will be data that matches
the uri.

Once Cassandra 0.7 is out the plan would be to create a small keyspace via the client that can be used to track keys deleted by the consumer,
so that smarter Key Ranges can be passed to get_range_slices.



##Example Config
----------------

You can either define beans of the proper name to have your own extractors (camel Expression) used, or you can override the name that will
be used to look up the extractor.

So for example, you could define your own extractors as follows

        <beans xmlns="http://www.springframework.org/schema/beans"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:camel="http://camel.apache.org/schema/spring"
               xsi:schemaLocation="
                  http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
                  http://camel.apache.org/schema/spring http://camel.apache.org/schema/spring/camel-spring.xsd">


             <camel:camelContext id="camel">

                     <camel:route>
                         <camel:from uri="direct:cassandraInbound"/>
                         <camel:to uri="cassandra://localhost:9160/camel-cassandra?columnFamilyExtractor=cfEx&amp;columnExtractor=colEx"/>
                     </camel:route>

             </camel:camelContext>

            <!-- the name of the columnFamilyExtractor and columnExtractor beans are specified above as options in the cassandra uri -->
            <bean id="cfEx" class="org.apache.camel.builder.ExpressionBuilder" factory-method="headerExpression">
                <constructor-arg index="0" type="java.lang.String" value="camel-cassandra-columnFamily"/>
            </bean>

            <bean id="colEx"  class="org.apache.camel.builder.ExpressionBuilder" factory-method="headerExpression">
                <constructor-arg index="0" type="java.lang.String" value="camel-cassandra-column"/>
            </bean>

            <!-- the keyExtractor and valueExtractor beans are found by their default name-->
            <bean id="keyExtractor" class="org.apache.camel.builder.ExpressionBuilder" factory-method="bodyOgnlExpression">
                <constructor-arg index="0" type="java.lang.String" value="getIdentifier()"/>
            </bean>

            <bean id="valueExtractor" class="org.apache.camel.builder.ExpressionBuilder" factory-method="bodyOgnlExpression">
                <constructor-arg index="0" type="java.lang.String" value="getImportantData().asByteArray()"/>
            </bean>
        </beans>


