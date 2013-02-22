package com.curtinhome.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example of a transactional Trident spout, based on the FixedBatchSpout that comes with Storm
 *
 * It expects a List of Value objects for each partition we want to simulate. This allows having
 * different data per partition and even different sizes of data per partition
 *
 */
public class TransactionalFixedBatchSpout implements IPartitionedTridentSpout<Integer, FixedBatchPartitionId, Map> {

    private List<List<Values>> m_data;
    private Fields m_outputFields;
    private int m_batchSize;

    public TransactionalFixedBatchSpout(Fields a_outputFields, int a_batchSize, List<List<Values>> a_data) {
        m_data = a_data;
        m_outputFields = a_outputFields;
        m_batchSize = a_batchSize;

    }


    class Coordinator implements IPartitionedTridentSpout.Coordinator<Integer> {
        @Override
        public Integer getPartitionsForBatch() {
            return m_data.size();
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }
    }

    class Emitter implements IPartitionedTridentSpout.Emitter<Integer, FixedBatchPartitionId, Map> {

        @Override
        public List<FixedBatchPartitionId> getOrderedPartitions(Integer partitions) {
            List<FixedBatchPartitionId> rtn = new ArrayList<FixedBatchPartitionId>(partitions);
            for (int i=0; i<partitions;i++) {
                rtn.add(new FixedBatchPartitionId(i));
            }
            return rtn;
        }

        @Override
        public Map emitPartitionBatchNew(TransactionAttempt a_attempt, TridentCollector a_collector, FixedBatchPartitionId a_partition, Map a_lastMeta) {
            Map<String, Integer> newMeta = new HashMap<String, Integer>();
            List<Values> data = m_data.get(a_partition.partition);

            int currentBlock = 0;
            if (a_lastMeta != null) {
                currentBlock = (Integer) a_lastMeta.get("currentBlock");
                int maxBlock = data.size() / m_batchSize;
                if (currentBlock + 1 >= maxBlock) {
                    currentBlock = 0;
                } else {
                    currentBlock += 1;
                }
            }
            emit(a_collector, data, currentBlock);

            newMeta.put("currentBlock", currentBlock);

            return newMeta;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt a_attempt, TridentCollector a_collector, FixedBatchPartitionId a_partition, Map a_meta) {
            int currentBlock = (Integer) a_meta.get("currentBlock");
            List<Values> data = m_data.get(a_partition.partition);
            emit(a_collector, data, currentBlock);
        }

        @Override
        public void close() {

        }

        @Override
        public void refreshPartitions(List<FixedBatchPartitionId> list) {

        }

        private void emit(TridentCollector a_collector, List<Values> a_data, int a_currentBlock) {
            int start = a_currentBlock * m_batchSize;
            int end = start + m_batchSize;

            for (int index = start; index < end; index++) {
                a_collector.emit(a_data.get(index));
            }
        }
    }


    @Override
    public IPartitionedTridentSpout.Coordinator<Integer> getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTridentSpout.Emitter<Integer, FixedBatchPartitionId, Map> getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }

    @Override
    public Fields getOutputFields() {
        return m_outputFields;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}