package org.dima.dominikschroeck.largestate.kafka.Partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Partition by id of the producer threads. In this way, we generate one partition for each  producer thread
 */
public class PSM_ParallelismPartitioner implements Partitioner{
    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        final List<PartitionInfo> partitionInfoList =
                cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfoList.size();

        final int normalPartitionCount = partitionCount;

        return ((Long) o).intValue() % normalPartitionCount;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
