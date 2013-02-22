package com.curtinhome.trident.spout;

import storm.trident.spout.ISpoutPartition;

public class FixedBatchPartitionId implements ISpoutPartition {
    public int partition;

    public FixedBatchPartitionId(int partition) {
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        FixedBatchPartitionId other = (FixedBatchPartitionId) o;
        return partition == other.partition;
    }

    @Override
    public int hashCode() {
        return 13  + partition;
    }

    @Override
    public String toString() {
        return getId();
    }

    @Override
    public String getId() {
        return  String.valueOf(partition);
    }
}