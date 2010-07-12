package org.apache.camel.component.cassandra;

import org.apache.camel.Exchange;
import org.apache.cassandra.thrift.Mutation;

import java.util.*;


/**
 *
 */
public class DataHolder {

    public Exchange exchange;
    public String keyspace;
    public Map<String, Map<String, List<Mutation>>> mutations = new HashMap<String, Map<String, List<Mutation>>>();

    public DataHolder() {
    }

    public DataHolder(Exchange e, String ks, String key, String family, Mutation m) {
        exchange = e;
        keyspace = ks;
        addMutation(key, family, m);
    }

    public void addMutation(String key, String columnfamily, Mutation mutation) {
        Map<String, List<Mutation>> columnfamilies = mutations.get(key);
        if (columnfamilies == null) {
            columnfamilies = new HashMap<String, List<Mutation>>();
            mutations.put(key, columnfamilies);
        }
        List<Mutation> mutations = columnfamilies.get(columnfamily);
        if (mutations == null) {
            mutations = new ArrayList<Mutation>();
            columnfamilies.put(columnfamily, mutations);
        }
        mutations.add(mutation);
    }

}
