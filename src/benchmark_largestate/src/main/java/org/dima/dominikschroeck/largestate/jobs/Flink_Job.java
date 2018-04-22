package org.dima.dominikschroeck.largestate.jobs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Abstract class for Flink benchmarking jobs. Defines the important parameters for setup. Implementations of this class can be executed by the Job_Starter Class.
 */
public abstract class Flink_Job {
    protected String KAFKA_SERVER;
    protected String ZOOKEEPER;

    protected String NAME;
    protected StreamExecutionEnvironment ENV;

    private Properties props;

    public Flink_Job() {
        super();
    }

    /**
     * @param KAFKA_SERVER Kafka Broker(s) to connect to. Multiple brokers comma-delimited
     * @param ZOOKEEPER    Zookeeper to connect to. Deprecated, rather use Kafka Brokers directly!
     * @param ENV          StreamExecutionEnvironment that the job uses. Setup does not happen in here but directly by Job Starter
     **/
    public Flink_Job(String KAFKA_SERVER, String ZOOKEEPER, String NAME, StreamExecutionEnvironment ENV) {
        this.KAFKA_SERVER = KAFKA_SERVER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.ENV = ENV;
        this.NAME = NAME;

    }

    public Flink_Job(Properties props, StreamExecutionEnvironment ENV) {
        this.props = props;
        this.ENV = ENV;
    }

    void runJob() {

    }
}
