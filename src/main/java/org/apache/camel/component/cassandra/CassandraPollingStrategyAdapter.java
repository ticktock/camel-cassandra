package org.apache.camel.component.cassandra;

import org.apache.camel.Exchange;
import org.apache.cassandra.thrift.Cassandra;

import java.util.Queue;

/**
 * Subclass this class if you only care to implement one type of polling method.
 */
public class CassandraPollingStrategyAdapter implements CassandraPollingStrategy {

    protected CassandraEndpoint cassandraEndpoint;

    @Override
    public void setEndpoint(CassandraEndpoint endpoint) {
        cassandraEndpoint = endpoint;
    }

    @Override
    public CassandraEndpoint getEndpoint() {
        return cassandraEndpoint;
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamily(Cassandra.Client cassandra, String keyspace, String columnFamily, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamily is not supported");
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamilyAndKey(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] key, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamilyandKey is not supported");
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamilySuperColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamilySuperColumn is not supported");
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamilySuperColumnAndKey(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, byte[] key, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamilySuperColumnAndKey is not supported");
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamilyColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] column, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamilyColumn is not supported");
    }

    @Override
    public Queue<DataHolder> pollKeyspaceColumnFamilySuperColumnAndColumn(Cassandra.Client cassandra, String keyspace, String columnFamily, byte[] supercolumn, byte[] column, int maxMessages) {
        throw new UnsupportedOperationException("pollKeyspaceColumnFamilySuperColumnAndColumn is not supported");
    }
}
