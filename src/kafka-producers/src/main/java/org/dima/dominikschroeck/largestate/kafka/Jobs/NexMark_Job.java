package org.dima.dominikschroeck.largestate.kafka.Jobs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewItemEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewPersonEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark.AuctionEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark.BidEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark.NewItemEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark.NewPersonEvent_Kafka_Serializer;
import org.dima.dominikschroeck.largestate.kafka.Monitoring.Production_Monitoring;
import org.dima.dominikschroeck.largestate.kafka.Partitioners.NexMark_ParallelismPartitioner;
import org.dima.dominikschroeck.largestate.kafka.Producers.NexMarkProducer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NexMark_Job extends Producer_Job {
    String KAFKA_SERVER;
    String ZOOKEEPER;
    int PARALLELISM;
    int EVENTSPERSTEP_MAX;
    int EVENTSPERSTEP_LOW;
    Long PAUSE;
    int CHANGEINTERVAL;



    /**
     * Static variables so producers only create auctions, persons and items for ID's that are available
     * Hence, I store the last ID given
     */
    public static Integer last_auction = 0;
    public static Integer last_person = 0;
    public static Integer last_item = 0;

    public static Object person_lock = new Object();
    public static Object auction_lock = new Object();
    public static Object item_lock = new Object();


    public NexMark_Job(String KAFKA_SERVER, String ZOOKEEPER, int PARALLELISM,int EVENTSPERSTEP_MAX, int EVENTSPERSTEP_LOW, Long PAUSE, int CHANGEINTERVAL) {
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

        System.out.println(("[INFO] Starting NexMark Event Generation. Varying bidding Throughtput every " + CHANGEINTERVAL / 60 / 1000 + " minutes. Other events are a fraction of the throughput as defined by the benchmark paper."));

        NexMarkProducer[] producers = new NexMarkProducer[PARALLELISM];

        ExecutorService threadPool = Executors.newFixedThreadPool(PARALLELISM+1);
        threadPool.execute(watcher);

        for (int i = 0; i < PARALLELISM; i++) {
            producers[i] = new NexMarkProducer(PAUSE, EVENTSPERSTEP_LOW, EVENTSPERSTEP_MAX,Long.valueOf(CHANGEINTERVAL), i);
            System.out.println("[INFO] Generating producer" + i);

            producers[i].setAuctionEventProducer ( create_AuctionEvent_Producer ( KAFKA_SERVER, NexMark_ParallelismPartitioner.class.getName (),AuctionEvent_Kafka_Serializer.class.getName () ) );
            producers[i].setBidEventProducer ( create_BidEvent_Producer ( KAFKA_SERVER, NexMark_ParallelismPartitioner.class.getName (),BidEvent_Kafka_Serializer.class.getName () ) );

            producers[i].setNewPersonEventProducer ( create_NewPersonEvent_Producer ( KAFKA_SERVER, NexMark_ParallelismPartitioner.class.getName (),NewPersonEvent_Kafka_Serializer.class.getName () ) );
            producers[i].setNewItemEventProducer ( create_NewItemEvent_Producer ( KAFKA_SERVER, NexMark_ParallelismPartitioner.class.getName (),NewItemEvent_Kafka_Serializer.class.getName()));

            threadPool.execute(producers[i]);
            watcher.registerProducer(producers[i]);

        }
    }

    public Producer<Long, AuctionEvent> create_AuctionEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<>(setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }

    public Producer<Long, BidEvent> create_BidEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<>(setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }

    public Producer<Long, NewPersonEvent> create_NewPersonEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<>(setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }

    public Producer<Long, NewItemEvent> create_NewItemEvent_Producer(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname) {
        return new KafkaProducer<>(setProperties ( KAFKA_SERVER,partitioner_classname,Deserializer_Classname ));
    }


    public Properties setProperties(String KAFKA_SERVER, String partitioner_classname, String Deserializer_Classname){
        Properties props = new Properties();
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
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
