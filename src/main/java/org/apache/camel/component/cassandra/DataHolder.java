package org.apache.camel.component.cassandra;

import org.apache.camel.Exchange;
import org.apache.cassandra.thrift.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 *
 */
public class DataHolder {

    static Logger log = LoggerFactory.getLogger(DataHolder.class);
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
        log.debug("addMutation({},{})", key, columnfamily);
        log.debug(mutation.toString());
        log.debug(new String(mutation.getDeletion().getPredicate().getColumn_namesIterator().next()));
        
        Map<String, List<Mutation>> columnfamilies = mutations.get(key);
        if (columnfamilies == null) {
            columnfamilies = new HashMap<String, List<Mutation>>();
            mutations.put(key, columnfamilies);
        }
        List<Mutation> colmutations = columnfamilies.get(columnfamily);
        if (colmutations == null) {
            colmutations = new ArrayList<Mutation>();
            columnfamilies.put(columnfamily, colmutations);
        }
        colmutations.add(mutation);
    }

}
