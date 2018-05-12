package org.dima.dominikschroeck.largestate.jobs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.PSM.ClickEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.PSM.ImpressionEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.PSM.SearchEvent_DeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM.ClickEvent_Kryo_FlinkInternalSerializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM.ImpressionEvent_Kryo_FlinkInternalSerializer;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM.SearchEvent_Kryo_FlinkInternalSerializer;
import org.dima.dominikschroeck.largestate.KeySelectors.Click_Product_Key_Selector;
import org.dima.dominikschroeck.largestate.KeySelectors.Impression_Product_Key_Selector;
import org.dima.dominikschroeck.largestate.KeySelectors.PSM_Search_Product_KeySelector;
import org.dima.dominikschroeck.largestate.functions.PSM.*;

import java.util.Properties;

/**
 * This Class includes the queries for the PSM Benchmark. It is a window-heavy benchmark that reflects common analytics queries to
 * Price Search Machines. We use Sliding/Tumbling windows as well as CoGroups and Window Joins.
 */
public class PSM_Job extends Flink_Job {


    private final String CLICK_TOPIC = "PSM_CLICKS";
    private final String IMPRESSION_TOPIC = "PSM_IMPRESSIONS";
    private final String SEARCH_TOPIC = "PSM_SEARCH";


    private String KAFKA_SERVER;
    private String ZOOKEEPER;
    private String NAME;
    private StreamExecutionEnvironment ENV;
    private Integer MAXPARALLELISM;
    private Properties props;

    public PSM_Job(String KAFKA_SERVER, String ZOOKEEPER, String NAME, StreamExecutionEnvironment ENV, Integer MAXPARALLELISM) {

        this.MAXPARALLELISM = MAXPARALLELISM;
        this.KAFKA_SERVER = KAFKA_SERVER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.NAME = NAME;
        this.ENV = ENV;
    }

    public PSM_Job(Properties props, StreamExecutionEnvironment ENV) {
        this.props = props;
        this.ENV = ENV;
    }

    /**
     * Implementation of abstract method.
     * Sets up Serialization, Data Sources, Queries and finally starts execution
     */
    public void runJob() {


        /**
         * Kafka Setup
         */

        /**
         * Stream Environment configuration. Registering the serializers in order to improve (de-)serialization of my custom POJOs
         */
        ENV.getConfig().enableForceKryo();
        ENV.getConfig().registerTypeWithKryoSerializer(PSM_ClickEvent.class, ClickEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(PSM_SearchEvent.class, SearchEvent_Kryo_FlinkInternalSerializer.class);
        ENV.getConfig().registerTypeWithKryoSerializer(PSM_ImpressionEvent.class, ImpressionEvent_Kryo_FlinkInternalSerializer.class);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", props.getProperty("kafka_server"));

        properties.setProperty("zookeeper.connect", props.getProperty("kafka_zookeeper"));
        properties.setProperty("group.id", "FlinkBenchmark");


        FlinkKafkaConsumer010<PSM_SearchEvent> SEARCH_CONSUMER = new FlinkKafkaConsumer010<>(SEARCH_TOPIC, new SearchEvent_DeserializationSchema(ENV.getConfig()), properties);

        FlinkKafkaConsumer010<PSM_ClickEvent> CLICK_CONSUMER = new FlinkKafkaConsumer010<>(CLICK_TOPIC, new ClickEvent_DeserializationSchema(ENV.getConfig()), properties);

        FlinkKafkaConsumer010<PSM_ImpressionEvent> IMPRESSION_CONSUMER = new FlinkKafkaConsumer010<>(IMPRESSION_TOPIC, new ImpressionEvent_DeserializationSchema(ENV.getConfig()), properties);


        /**
         * Timestamp Extractors from Kafka Events
         */
        SEARCH_CONSUMER.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PSM_SearchEvent>() {
            @Override
            public long extractAscendingTimestamp(PSM_SearchEvent element) {
                return element.getTimestamp();
            }
        });
        SEARCH_CONSUMER.setStartFromEarliest();

        CLICK_CONSUMER.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PSM_ClickEvent>() {
            @Override
            public long extractAscendingTimestamp(PSM_ClickEvent element) {
                return element.getTimestamp();
            }
        });
        CLICK_CONSUMER.setStartFromEarliest();

        IMPRESSION_CONSUMER.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PSM_ImpressionEvent>() {
            @Override
            public long extractAscendingTimestamp(PSM_ImpressionEvent element) {
                return element.getTimestamp();
            }
        });
        IMPRESSION_CONSUMER.setStartFromEarliest();


        /**
         * Input Streams from Kafka Consumer
         */


        // Search input stream
        Integer query_para = props.getProperty("Search_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Search_Input"));
        DataStream<PSM_SearchEvent> search_stream = ENV.addSource(SEARCH_CONSUMER, "SEARCH")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        //Impression Input Stream
        query_para = props.getProperty("Impressions_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Impressions_Input"));
        DataStream<PSM_ImpressionEvent> impression_stream = ENV.addSource(IMPRESSION_CONSUMER, "IMPRESSIONS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        // Click input stream
        query_para = props.getProperty("Clicks_Input") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Clicks_Input"));
        DataStream<PSM_ClickEvent> click_stream = ENV.addSource(CLICK_CONSUMER, "CLICKS")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * HERE GO QUERY DESCRIPTIONS AND IMPLEMENTATIONS
         */

        /**
         * Query 1: Best performing category(ies) in last 10 minutes by shop owner in terms of clicks
         * OUT: Timestamp, Window end, Category, Owner, Clicks , Latest Ingestion Time timestamp(Might be more than one category!)
         */
        query_para = props.getProperty("Query1_WindowFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query1_WindowFunction"));
        DataStream<Tuple6<Long, Long, String, String, Long, Long>> best_performance_by_category_and_owner = click_stream
                .keyBy("owner")
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .apply(new Query1_Windowfunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .name("Query1_WindowFunction")
                .uid("Query1_WindowFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Query 2: Median Cost per Product (CountWindow)
         * OUT: Timestamp, Product, AVG(COST), Latest Ingestion Time timestamp
         */
        query_para = props.getProperty("Query2_RichWindowFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query2_RichWindowFunction"));
        DataStream<Tuple4<Long, String, Double, Long>> median_cost_per_product = click_stream
                .keyBy("product")
                .countWindow(10000, 1000)
                .apply(new Query2_RichWindowFunction(50,query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query2_RichWindowFunction")
                .name("Query2_RichWindowFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Query 3: Median Cost per Shop per Minute
         * OUT: Window start, Window end, Shop Owner, SUM(CPC)
         */

        query_para = props.getProperty("Query3_RichWindowFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query3_RichWindowFunction"));
        DataStream<Tuple4<Long, String, Double, Long>> cost_per_shop = click_stream
                .keyBy("owner")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new Query3_RichWindowFunction(50,query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query3_RichWindowFunction")
                .name("Query3_RichWindowFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Query 4: Impressions to Clicks by Product over the last 5 minutes sliding by 1 minute
         * OUT: Timestamp, Product, Impressions / Click, Latest Timestamp
         */

        DataStream<Tuple4<Long, String, Double, Long>> impressions_to_click = click_stream.coGroup(impression_stream)
                .where(new Click_Product_Key_Selector())
                .equalTo(new Impression_Product_Key_Selector())
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .apply(new Query4_RichCoGroupFunction(ENV.getParallelism()));


        /**
         * Query 5: Search to Clicks by Product (Window CoGroup)
         * OUT: Timestamp, Product, Search to Clicks
         */
        DataStream<Tuple4<Long, String, Double, Long>> search_to_clicks_join =
                click_stream
                        .coGroup(search_stream)
                        .where(new Click_Product_Key_Selector()).equalTo(new PSM_Search_Product_KeySelector())
                        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                        .apply(new Query5_RichCoGroupFunction(ENV.getParallelism()));


        /**
         * Kafka output
         */
        FlinkKafkaProducer010<String> sink = new FlinkKafkaProducer010<String>(props.getProperty("kafka_server"), "psm_results", new SimpleStringSchema());



        /**
         * Actual Code for Sinks and Overall Latencies (Query 5 - 1)
         */

        query_para = props.getProperty("Sink_5_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_5_Parallelism"));
        search_to_clicks_join.map(new RichMapFunction<Tuple4<Long, String, Double, Long>, String>() {

            @Override
            public String map(Tuple4<Long, String, Double, Long> longStringDoubleLongTuple4) {

                return "Query5," + longStringDoubleLongTuple4.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query5_MetricsMap")
                .addSink(sink)
                .name("Query5_Sink")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para);


        query_para = props.getProperty("Sink_4_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_4_Parallelism"));
        impressions_to_click.map(new RichMapFunction<Tuple4<Long, String, Double, Long>, String>() {

            @Override
            public String map(Tuple4<Long, String, Double, Long> longStringDoubleLongTuple4) {
                return "Query4," + longStringDoubleLongTuple4.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query4_MetricsMap")
                .addSink(sink)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query4_Sink")
        ;

        query_para = props.getProperty("Sink_3_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_3_Parallelism"));
        cost_per_shop.map(new RichMapFunction<Tuple4<Long, String, Double, Long>, String>() {

            @Override
            public String map(Tuple4<Long, String, Double, Long> longLongStringDoubleLongTuple5) {
                return "Query3," + longLongStringDoubleLongTuple5.toString();
            }
        })
                .name("Query3_MetricsMap")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .addSink(sink)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query3_Sink");

        query_para = props.getProperty("Sink_2_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_2_Parallelism"));
        median_cost_per_product.map(new RichMapFunction<Tuple4<Long, String, Double, Long>, String>() {
            @Override
            public String map(Tuple4<Long, String, Double, Long> longStringDoubleLongTuple4) {
                return "Query2," + longStringDoubleLongTuple4.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query2_MetricsMap")
                .addSink(sink)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query2_Sink")
        ;

        query_para = props.getProperty("Sink_1_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_1_Parallelism"));
        best_performance_by_category_and_owner.map(new RichMapFunction<Tuple6<Long, Long, String, String, Long, Long>, String>() {
            @Override
            public String map(Tuple6<Long, Long, String, String, Long, Long> longLongStringStringLongLongTuple6) {
                return "Query1," + longLongStringStringLongLongTuple6.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query1_MetricsMap")
                .addSink(sink)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query1_Sink")
        ;


        try {
            ENV.execute(props.getProperty("experiment_name"));
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

