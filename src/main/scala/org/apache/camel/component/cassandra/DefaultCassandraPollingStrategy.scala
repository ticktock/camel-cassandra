package org.apache.camel.component.cassandra

import org.apache.cassandra.thrift.Cassandra.Client
import java.lang.String
import reflect.BeanProperty
import org.apache.cassandra.thrift._
import collection.JavaConversions._
import com.shorrockin.cascal.utils.Conversions._
import org.apache.camel.component.cassandra.CassandraComponent._
import org.apache.camel.{ExchangePattern, Exchange}
import java.util.{Collections, List, LinkedList, Queue}

/**
 *
 */

class DefaultCassandraPollingStrategy(var endpoint: CassandraEndpoint) extends CassandraPollingStrategy {
  def getEndpoint = endpoint

  def setEndpoint(point: CassandraEndpoint) = {endpoint = point}


  /**
   * Calls get_range_slices with a key range with empty start and end keys
   */

  def pollKeyspaceColumnFamilySuperColumnAndColumn(cassandra: Client, keyspace: String, columnFamily: String, supercolumn: Array[Byte], column: Array[Byte], maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    slicePredicate.setColumn_names(Collections.singletonList(column))
    val keyRange = new KeyRange()
    keyRange.setStart_key("").setEnd_key("").setCount(endpoint.pollingMaxKeyRange)
    val parent = new ColumnParent(columnFamily)
    parent.setSuper_column(supercolumn)
    val keySlices: List[KeySlice] = cassandra.get_range_slices(keyspace, parent, slicePredicate, keyRange, endpoint.getThriftConsistency)
    keySlices foreach {
      keySlice: KeySlice => {
        (keySlice.getColumns) foreach {
          colOrSuperCol: ColumnOrSuperColumn => {
            val exchange = endpoint.createExchange(ExchangePattern.InOnly)
            val msg = exchange.getIn
            msg.setHeader(keyspaceHeader, keyspace)
            msg.setHeader(columnFamilyHeader, columnFamily)
            msg.setHeader(keyHeader, keySlice.getKey)
            msg.setHeader(superColumnHeader, string(supercolumn))
            msg.setHeader(columnHeader, string(colOrSuperCol.getColumn.getName))
            msg.setBody(endpoint.unmarshalIfPossible(colOrSuperCol.getColumn.getValue))
            data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, keySlice.getKey, columnFamily, supercolumn, column))
          }
        }
      }
    }
    data
  }

  def pollKeyspaceColumnFamilySuperColumnAndKey(cassandra: Client, keyspace: String, columnFamily: String, supercolumn: Array[Byte], key: Array[Byte], maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    val sliceRange = new SliceRange
    sliceRange.setStart(new Array[Byte](0)).setFinish(new Array[Byte](0)).setCount(maxMessages)
    slicePredicate.setSlice_range(sliceRange)
    val parent = new ColumnParent(columnFamily)
    parent.setSuper_column(supercolumn)
    val colOrSuperCols: List[ColumnOrSuperColumn] = cassandra.get_slice(keyspace, key, parent, slicePredicate, endpoint.getThriftConsistency)
    colOrSuperCols foreach {
      colOrSuperCol: ColumnOrSuperColumn => {
        val exchange = endpoint.createExchange(ExchangePattern.InOnly)
        val msg = exchange.getIn
        msg.setHeader(keyspaceHeader, keyspace)
        msg.setHeader(columnFamilyHeader, columnFamily)
        msg.setHeader(keyHeader, key)
        msg.setHeader(superColumnHeader, string(supercolumn))
        msg.setHeader(columnHeader, string(colOrSuperCol.getColumn.getName))
        msg.setBody(endpoint.unmarshalIfPossible(colOrSuperCol.getColumn.getValue))
        data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, key, columnFamily, supercolumn, colOrSuperCol.getColumn.getName))
      }
    }
    data
  }

  /**
   * Calls get_range_slices with a key range with empty start and end keys
   */
  def pollKeyspaceColumnFamilySuperColumn(cassandra: Client, keyspace: String, columnFamily: String, supercolumn: Array[Byte], maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    val sliceRange = new SliceRange
    sliceRange.setStart(new Array[Byte](0)).setFinish(new Array[Byte](0)).setCount(endpoint.pollingMaxKeyRange)
    slicePredicate.setSlice_range(sliceRange)
    val keyRange = new KeyRange()
    keyRange.setStart_key("").setEnd_key("").setCount(endpoint.pollingMaxKeyRange)
    val parent = new ColumnParent(columnFamily)
    parent.setSuper_column(supercolumn)
    val keySlices: List[KeySlice] = cassandra.get_range_slices(keyspace, parent, slicePredicate, keyRange, endpoint.getThriftConsistency)
    keySlices foreach {
      keySlice: KeySlice => {
        (keySlice.getColumns) foreach {
          colOrSuperCol: ColumnOrSuperColumn => {
            val col = colOrSuperCol.getColumn
            val exchange = endpoint.createExchange(ExchangePattern.InOnly)
            val msg = exchange.getIn
            msg.setHeader(keyspaceHeader, keyspace)
            msg.setHeader(columnFamilyHeader, columnFamily)
            msg.setHeader(keyHeader, keySlice.getKey)
            msg.setHeader(superColumnHeader, string(supercolumn))
            msg.setHeader(columnHeader, string(col.getName))
            msg.setBody(endpoint.unmarshalIfPossible(col.getValue))
            data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, keySlice.getKey, columnFamily, supercolumn, col.getName))
          }
        }
      }
    }
    data
  }

  /**
   * Calls get_range_slices with a key range with empty start and end keys
   */
  def pollKeyspaceColumnFamilyColumn(cassandra: Client, keyspace: String, columnFamily: String, column: Array[Byte], maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    slicePredicate.setColumn_names(Collections.singletonList(column))
    val keyRange = new KeyRange()
    keyRange.setStart_key("").setEnd_key("").setCount(endpoint.pollingMaxKeyRange)
    val keySlices: List[KeySlice] = cassandra.get_range_slices(keyspace, new ColumnParent(columnFamily), slicePredicate, keyRange, endpoint.getThriftConsistency)
    keySlices foreach {
      keySlice: KeySlice => {
        (keySlice.getColumns) foreach {
          colOrSuperCol: ColumnOrSuperColumn => {
            val exchange = endpoint.createExchange(ExchangePattern.InOnly)
            val msg = exchange.getIn
            msg.setHeader(keyspaceHeader, keyspace)
            msg.setHeader(columnFamilyHeader, columnFamily)
            msg.setHeader(keyHeader, keySlice.getKey)
            msg.setHeader(columnHeader, string(colOrSuperCol.getColumn.getName))
            msg.setBody(endpoint.unmarshalIfPossible(colOrSuperCol.getColumn.getValue))
            data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, keySlice.getKey, columnFamily, colOrSuperCol.getColumn.getName))
          }
        }
      }
    }
    data
  }


  def pollKeyspaceColumnFamilyAndKey(cassandra: Client, keyspace: String, columnFamily: String, key: Array[Byte], maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    val sliceRange = new SliceRange
    sliceRange.setStart(new Array[Byte](0)).setFinish(new Array[Byte](0)).setCount(maxMessages)
    slicePredicate.setSlice_range(sliceRange)
    val colOrSuperCols: List[ColumnOrSuperColumn] = cassandra.get_slice(keyspace, key, new ColumnParent(columnFamily), slicePredicate, endpoint.getThriftConsistency)
    colOrSuperCols foreach {
      colOrSuperCol: ColumnOrSuperColumn => {
        val exchange = endpoint.createExchange(ExchangePattern.InOnly)
        val msg = exchange.getIn
        msg.setHeader(keyspaceHeader, keyspace)
        msg.setHeader(columnFamilyHeader, columnFamily)
        msg.setHeader(keyHeader, key)
        msg.setHeader(columnHeader, string(colOrSuperCol.getColumn.getName))
        msg.setBody(endpoint.unmarshalIfPossible(colOrSuperCol.getColumn.getValue))
        data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, key, columnFamily, colOrSuperCol.getColumn.getName))
      }
    }
    data
  }

  /**
   * Calls get_range_slices with a key range with empty start and end keys
   */
  def pollKeyspaceColumnFamily(cassandra: Client, keyspace: String, columnFamily: String, maxMessages: Int): Queue[DataHolder] = {
    val data = new LinkedList[DataHolder]
    val slicePredicate = new SlicePredicate
    val sliceRange = new SliceRange
    sliceRange.setStart(new Array[Byte](0)).setFinish(new Array[Byte](0)).setCount(maxMessages)
    slicePredicate.setSlice_range(sliceRange)
    val keyRange = new KeyRange()
    keyRange.setStart_key("").setEnd_key("").setCount(endpoint.pollingMaxKeyRange)
    val keySlices: List[KeySlice] = cassandra.get_range_slices(keyspace, new ColumnParent(columnFamily), slicePredicate, keyRange, endpoint.getThriftConsistency)
    keySlices foreach {
      keySlice: KeySlice => {
        (keySlice.getColumns) foreach {
          colOrSuperCol: ColumnOrSuperColumn => {
            val exchange = endpoint.createExchange(ExchangePattern.InOnly)
            val msg = exchange.getIn
            msg.setHeader(keyspaceHeader, keyspace)
            msg.setHeader(columnFamilyHeader, columnFamily)
            msg.setHeader(keyHeader, keySlice.getKey)
            msg.setHeader(columnHeader, string(colOrSuperCol.getColumn.getName))
            msg.setBody(endpoint.unmarshalIfPossible(colOrSuperCol.getColumn.getValue))
            data.add(dataHolderWithSingleColumnDeteltion(exchange, keyspace, keySlice.getKey, columnFamily, colOrSuperCol.getColumn.getName))
          }
        }
      }
    }
    data
  }

  def dataHolderWithSingleColumnDeteltion(exchange: Exchange, keyspace: String, key: String, columnFamily: String, column: Array[Byte]): DataHolder = {
    new DataHolder(exchange, keyspace, key, columnFamily, new Mutation().setDeletion(new Deletion().setPredicate(new SlicePredicate().setColumn_names(Collections.singletonList(column))).setTimestamp(System.currentTimeMillis)))
  }

  def dataHolderWithSingleColumnDeteltion(exchange: Exchange, keyspace: String, key: String, columnFamily: String, supercolumn: Array[Byte], column: Array[Byte]) = {
    new DataHolder(exchange, keyspace, key, columnFamily, new Mutation().setDeletion(new Deletion().setSuper_column(supercolumn).setPredicate(new SlicePredicate().setColumn_names(Collections.singletonList(column))).setTimestamp(System.currentTimeMillis)))
  }

}