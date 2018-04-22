package org.dima.dominikschroeck.largestate.kafka.Jobs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.PSM.ClickEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.PSM.ImpressionEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.PSM.SearchEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.kafka.Monitoring.Production_Monitoring;
import org.dima.dominikschroeck.largestate.kafka.Partitioners.PSM_ParallelismPartitioner;
import org.dima.dominikschroeck.largestate.kafka.Producers.PSM_Producer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PSM_Job extends Producer_Job  {

    String KAFKA_SERVER;
    String ZOOKEEPER;
    int PARALLELISM;
    int EVENTSPERSTEP_MAX;
    int EVENTSPERSTEP_LOW;
    Long PAUSE;
    int CHANGEINTERVAL;


    public PSM_Job(String KAFKA_SERVER, String ZOOKEEPER, int PARALLELISM,int EVENTSPERSTEP_MAX, int EVENTSPERSTEP_LOW, Long PAUSE, int CHANGEINTERVAL) {
        super();
        this.KAFKA_SERVER = KAFKA_SERVER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.PARALLELISM = PARALLELISM;
        this.EVENTSPERSTEP_MAX = EVENTSPERSTEP_MAX;
        this.EVENTSPERSTEP_LOW = EVENTSPERSTEP_LOW;
        this.PAUSE = PAUSE;
        this.CHANGEINTERVAL = CHANGEINTERVAL;
    }

    public void runJob(){

        Production_Monitoring watcher = new Production_Monitoring ();

        PSM_Producer[] producers = new PSM_Producer[PARALLELISM];
        ExecutorService threadPool = Executors.newFixedThreadPool(PARALLELISM+1);
        for (int i = 0; i < PARALLELISM; i++) {
            //Long pause, Long changeEvery, int eventsPerStep_max, int eventsPerStep_low, int eventsPerStep_initial, int id, Producer producer, String URL)
            producers[i] = new PSM_Producer(PAUSE,Long.valueOf(CHANGEINTERVAL),EVENTSPERSTEP_MAX,EVENTSPERSTEP_LOW,EVENTSPERSTEP_LOW,i,"https://stefanie.dominikschroeck.de/products_categories_owners.csv");
            producers[i].setPSM_ClickEvent_Producer ( create_PSM_ClickEvent_Producer ( KAFKA_SERVER, PSM_ParallelismPartitioner.class.getName (), ClickEvent_Kafka_Serializer.class.getName () ) );
            producers[i].setPSM_SearchEvent_Producer ( create_PSM_SearchEvent_Producer( KAFKA_SERVER, PSM_ParallelismPartitioner.class.getName (), SearchEvent_Kafka_Serializer.class.getName () ) );
            producers[i].setPSM_ImpressionEvent_Producer ( create_PSM_ImpressionEvent_Producer ( KAFKA_SERVER, PSM_ParallelismPartitioner.class.getName (), ImpressionEvent_Kafka_Serializer.class.getName ()) );
            threadPool.execute(producers[i]);
            watcher.registerProducer( producers[i] );
        }
        threadPool.execute ( watcher );
    }

    public Producer<Long, PSM_ClickEvent> create_PSM_ClickEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<> (setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }

    public Producer<Long, PSM_SearchEvent> create_PSM_SearchEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<> (setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }

    public Producer<Long, PSM_ImpressionEvent> create_PSM_ImpressionEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<> (setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }




    public Properties setProperties(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname){
        Properties props = new Properties();
        props.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                Deserializer_Classname);
        props.put("partitioner.class", partitioner_classname);
        return props;
    }
}
