package org.dima.dominikschroeck.largestate.kafka.Jobs;


public abstract class Producer_Job {
    private String KAFKA_SERVER;
    private String ZOOKEEPER;
    private int PARALLELISM;
    private int CHECKPOINTING_INTERVAL;

    public Producer_Job(){
        super();
    }


    void runJob(){


    }
    /*public Producer<Long, String> createProducer(String KAFKA_SERVER,String classname) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                PSM_Event_Serializer.class.getName());
        props.put("partitioner.class", classname);
        return new KafkaProducer<>(props);
    }*/


}
