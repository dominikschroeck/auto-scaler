package org.dima.dominikschroeck.largestate.kafka.Jobs;


public abstract class Producer_Job {
    private String KAFKA_SERVER;
    private String ZOOKEEPER;
    private int PARALLELISM;
    private int CHECKPOINTING_INTERVAL;

    public Producer_Job(){
        super();
    }


    /**
     * Implement this method to build your own producer for a benchmark
     */
    void runJob(){


    }



}
