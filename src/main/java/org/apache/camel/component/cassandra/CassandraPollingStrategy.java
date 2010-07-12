package org.apache.camel.component.cassandra;

import org.apache.camel.Exchange;
import org.apache.cassandra.thrift.Cassandra;

import java.util.Queue;

/**
 */
public interface CassandraPollingStrategy {

    void setEndpoint(CassandraEndpoint endpoint);

    CassandraEndpoint getEndpoint();

    Queue<DataHolder> pollKeyspaceColumnFamily(Cassandra.Client cassandra, String keyspace, String columnFamily, int maxMessages);

    Queue<DataHolder> pollKeyspaceColumnFamilyAndKey(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] key, int maxMessages);

    Queue<DataHolder> pollKeyspaceColumnFamilyColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] column, int maxMessages);

    Queue<DataHolder> pollKeyspaceColumnFamilySuperColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, int maxMessages);

    Queue<DataHolder> pollKeyspaceColumnFamilySuperColumnAndKey(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, byte[] key, int maxMessages);

    Queue<DataHolder> pollKeyspaceColumnFamilySuperColumnAndColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, byte[] column, int maxMessages);


}
