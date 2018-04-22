package org.dima.dominikschroeck.largestate.jobs;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.dima.dominikschroeck.largestate.Events.NexMark.*;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.NexMark.AuctionEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.NexMark.BidEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.NexMark.NewItemEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.NexMark.NewPersonEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.NexMark.*;
import org.dima.dominikschroeck.largestate.queries.NexMark.*;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Job implementation of NexMark benchmark. We only implemented Queries 4-8 as these are actual real-time continuous queries.
 */
public class NexMark_Job extends Flink_Job {

    private final String AUCTION_TOPIC = "auctionEvents";
    private final String BID_TOPIC = "bidEvents";
    private final String ITEM_TOPIC = "itemEvents";
    private final String PERSON_TOPIC = "personEvents";
    Integer MAXPARALLELISM;
    private String KAFKA_SERVER;
    private String ZOOKEEPER;
    private StreamExecutionEnvironment ENV;
    private String NAME;
    private Properties props;

    public NexMark_Job(String KAFKA_SERVER, String ZOOKEEPER, String NAME, StreamExecutionEnvironment ENV, Integer MAXPARALLELISM) {

        this.MAXPARALLELISM = MAXPARALLELISM;
        this.KAFKA_SERVER = KAFKA_SERVER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.NAME = NAME;
        this.ENV = ENV;

        ENV.getConfig().registerTypeWithKryoSerializer(AuctionEvent.class, AuctionEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(NewPersonEvent.class, NewPersonEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(NewItemEvent.class, NewItemEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(BidEvent.class, BidEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(AuctionEvent_Category.class, AuctionEvent_Category_Kryo_FlinkInternalSerializer.class);
    }

    public NexMark_Job(Properties props, StreamExecutionEnvironment ENV) {
        this.props = props;
        this.ENV = ENV;

        ENV.getConfig().registerTypeWithKryoSerializer(AuctionEvent.class, AuctionEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(NewPersonEvent.class, NewPersonEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(NewItemEvent.class, NewItemEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(BidEvent.class, BidEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(AuctionEvent_Category.class, AuctionEvent_Category_Kryo_FlinkInternalSerializer.class);
    }

    /**
     * Runs the Job with given parameters. More on queries and background to be found in thesis as well as Paper regarding the benchmark.
     */
    public void runJob() {


        /**
         * Assuming as many partitions in Kafka Topic as Parallelism!
         */


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", props.getProperty("kafka_server"));

        properties.setProperty("zookeeper.connect", props.getProperty("kafka_zookeeper"));
        properties.setProperty("group.id", "FlinkBenchmark_MAS");
        FlinkKafkaConsumer010<AuctionEvent> auction_consumer = (new FlinkKafkaConsumer010<>(AUCTION_TOPIC, new AuctionEvent_DeserializationSchema(), properties));
        FlinkKafkaConsumer010<BidEvent> bid_consumer = (new FlinkKafkaConsumer010<>(BID_TOPIC, new BidEvent_DeserializationSchema(), properties));
        FlinkKafkaConsumer010<NewPersonEvent> person_consumer = (new FlinkKafkaConsumer010<>(PERSON_TOPIC, new NewPersonEvent_DeserializationSchema(), properties));
        FlinkKafkaConsumer010<NewItemEvent> item_consumer = (new FlinkKafkaConsumer010<>(ITEM_TOPIC, new NewItemEvent_DeserializationSchema(), properties));

        FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<String>(props.getProperty("kafka_server"), "nexmark_results", new SimpleStringSchema());


        auction_consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<AuctionEvent>() {
            @Nullable

            private long timestamp;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(this.timestamp);
            }

            @Override
            public long extractTimestamp(AuctionEvent element, long l) {
                this.timestamp = element.getTimestamp();
                return timestamp;
            }
        });


        bid_consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<BidEvent>() {
            @Nullable

            private long timestamp;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(this.timestamp);
            }

            @Override
            public long extractTimestamp(BidEvent element, long l) {
                this.timestamp = element.getTimestamp();
                return timestamp;
            }
        });

        person_consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<NewPersonEvent>() {
            @Nullable

            private long timestamp;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(this.timestamp);
            }

            @Override
            public long extractTimestamp(NewPersonEvent element, long l) {
                this.timestamp = element.getTimestamp();
                return timestamp;
            }
        });


        item_consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<NewItemEvent>() {
            @Nullable

            private long timestamp;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(this.timestamp);
            }

            @Override
            public long extractTimestamp(NewItemEvent newItemEvent, long l) {
                this.timestamp = newItemEvent.getTimestamp();
                return timestamp;
            }
        });


        /**
         * Set start offset (Here we start just from the earliest)
         */
        if (!props.getProperty("restarted").equals("true")) {
            auction_consumer.setStartFromEarliest();
            bid_consumer.setStartFromEarliest();
            person_consumer.setStartFromEarliest();
            item_consumer.setStartFromEarliest();
        }


        /**
         * Incoming Data Streams. Taking the incoming kafka stream and decode the corresponding Events into POJOs
         */

        Integer query_para = props.getProperty("Auctions_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Auctions_Input"));
        DataStream<AuctionEvent> openAuctions = ENV.addSource(auction_consumer, "AUCTIONS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        query_para = props.getProperty("Item_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Item_Input"));
        DataStream<NewItemEvent> NewItems = ENV.addSource(item_consumer, "ITEMS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());
        ;


        query_para = props.getProperty("Person_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Person_Input"));
        DataStream<NewPersonEvent> NewPerson = ENV.addSource(person_consumer, "NEW PERSONS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());

        query_para = props.getProperty("Bid_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Bid_Input"));
        DataStream<BidEvent> BidStream = ENV.addSource(bid_consumer, "BIDS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Query DataStreams Query 4: Average Price per Category
         */

        if (props.getProperty("Query4_Active").equals("true")) {
            Query4_NexMark query4NexMark = new Query4_NexMark(ENV, props, producer);
            query4NexMark.invoke(openAuctions, NewItems, BidStream);
        }

        /**
         *
         *
         * Query 5: Hottest Item (Item with most bids)
         */

        if (props.getProperty("Query5_Active").equals("true")) {
            Query5_NexMark query5NexMark = new Query5_NexMark(ENV, props, producer);
            Time windowSize = Time.seconds(10); // Timewindow size (EventTime!)
            Time slide = Time.seconds(1); // Slide by
            query5NexMark.invoke(BidStream, openAuctions, windowSize, slide);

        }

        /*
         * Query 6: Seller average price for last n auctions (only closed auctions)
         */


        if (props.getProperty("Query6_Active").equals("true")) {
            Query6_NexMark query6NexMark = new Query6_NexMark(ENV, props, producer);
            Integer windowSize = 1000; // windowSize for Count Window in No. Elements
            Integer slide = 10; // Number of Events to slide by
            query6NexMark.invoke(BidStream, openAuctions, windowSize, slide);
        }



        /*
         * * Query 7: Items with highest bid (auction active in last 10 minutes)
         */


        if (props.getProperty("Query7_Active").equals("true")) {
            Query7_NexMark query7NexMark = new Query7_NexMark(ENV, props, producer);
            Time windowSize = Time.seconds(10);
            query7NexMark.invoke(BidStream, openAuctions, windowSize);

        }


        /**
         * Query 8: Window join n minutes: New users that opened an auction
         */

        if (props.getProperty("Query8_Active").equals("true")) {
            Query8_NexMark query8NexMark = new Query8_NexMark(ENV, props, producer);
            Time windowSize = Time.minutes(1);
            query8NexMark.invoke(openAuctions, NewPerson, windowSize);
        }


        try {
            ENV.execute(props.getProperty("experiment_name"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
