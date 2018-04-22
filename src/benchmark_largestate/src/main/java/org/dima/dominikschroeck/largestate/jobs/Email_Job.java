package org.dima.dominikschroeck.largestate.jobs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.dima.dominikschroeck.largestate.functions.Email.Query1_CTR_FlatMap;
import org.dima.dominikschroeck.largestate.functions.Email.Query2_AllWindowFunction;

import java.util.Properties;

public class Email_Job extends Flink_Job {

    private String KAFKA_SERVER;
    private String ZOOKEEPER;
    private int PARALLELISM;
    private int CHECKPOINTING_INTERVAL;
    private String NAME;
    private Integer LATENCY_TRACKING_INTERVAL;


  /*  public Email_Job(String KAFKA_SERVER, String ZOOKEEPER, int PARALLELISM, int CHECKPOINTING_INTERVAL, String NAME, Integer LATENCY_TRACKING_INTERVAL) {

        this.KAFKA_SERVER = KAFKA_SERVER;
        this.ZOOKEEPER = ZOOKEEPER;
        this.PARALLELISM = PARALLELISM;
        this.CHECKPOINTING_INTERVAL = CHECKPOINTING_INTERVAL;
        this.NAME = NAME;
        this.LATENCY_TRACKING_INTERVAL = LATENCY_TRACKING_INTERVAL;

    }*/


    public void runJob() {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
        env.setParallelism(PARALLELISM);


        /**
         * Kafka Data Source. Topic: "emailEvents". Events of form 'EventType', EVENT PARAMETERS
         * Events can be: "SENT","OPEN","CLICK", "DELIVERED"
         * "SENT" captures the amount of emails sent in a time interval and the respective array of IDs
         * Same for "DELIVERED"
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);

        properties.setProperty("zookeeper.connect", ZOOKEEPER);
        properties.setProperty("group.id", "FlinkBenchmark");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("emailEvents", new SimpleStringSchema(), properties);

        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String s) {
                return Long.valueOf(s.split(",")[0]).longValue();
            }
        });

        consumer.setStartFromEarliest();


        DataStream<String> input_stream = env
                .addSource(consumer);


        DataStream<Tuple5<Long, String, String, String, Long>> click_stream = input_stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) {
                        return s.contains("CLICK");
                    }
                })
                .map(new RichMapFunction<String, Tuple5<Long, String, String, String, Long>>() {
                    @Override
                    public Tuple5<Long, String, String, String, Long> map(String s) {
                        String[] elem = s.split(",");
                        // Last currentTime is Ingestion Time, NEVER EVER TO BE CHANGED!
                        return new Tuple5<>(System.currentTimeMillis(), elem[1], elem[2], elem[3], System.currentTimeMillis());
                    }
                });

        DataStream<Tuple5<Long, String, String, String, Long>> impression_stream = input_stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) {
                        return s.contains("CLICK");
                    }
                })
                .map(new RichMapFunction<String, Tuple5<Long, String, String, String, Long>>() {
                    @Override
                    public Tuple5<Long, String, String, String, Long> map(String s) {
                        String[] elem = s.split(",");
                        // Last currentTime is Ingestion Time, NEVER EVER TO BE CHANGED!
                        return new Tuple5<>(System.currentTimeMillis(), elem[1], elem[2], elem[3], System.currentTimeMillis());
                    }
                });


        /**
         * Query 1: 99th quantile CTR for each Email campaign (Running) (Click / Opened)
         * COUNT(Click)/COUNT(Opened) by Campaign
         * IN: Tuple4<TimeStamp,Campaign_Name,Email,Type
         */
        DataStream<Tuple4<Long, String, String, String>> events_to_tuples = input_stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) {
                        return s.contains("OPEN") || s.contains("CLICK");
                    }
                })
                .map(new MapFunction<String, Tuple4<Long, String, String, String>>() {
                    @Override
                    public Tuple4<Long, String, String, String> map(String s) {
                        String[] elem = s.split(",");
                        return new Tuple4<>(System.currentTimeMillis(), elem[1], elem[2], elem[3]);
                    }
                });

        // IN Tuple4<Long,String,String,String>
        // OUT: Long,String, Double (Double ist Average)
        DataStream<Tuple3<Long, String, Double>> CTR = events_to_tuples.keyBy(1)
                .map(new Query1_CTR_FlatMap());


        /**
         * Query 2: Average Open Rate per campaign over 60 minutes (Opened / Sent)
         */
        DataStream<Tuple4<Long, String, String, String>> open_send_to_tuples = input_stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) {
                        return s.contains("OPEN") || s.contains("SEND");
                    }
                })
                .map(new MapFunction<String, Tuple4<Long, String, String, String>>() {
                    @Override
                    public Tuple4<Long, String, String, String> map(String s) {
                        String[] elem = s.split(",");
                        return new Tuple4<>(System.currentTimeMillis(), elem[1], elem[2], elem[3]);
                    }
                });

        DataStream<Tuple4<Long, Long, String, Double>> openrate = open_send_to_tuples.keyBy(1).window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
                .apply(new Query2_AllWindowFunction());


        // WRITE BACK TO KAFKA
        FlinkKafkaProducer010<Tuple3<Long, String, Double>> kafka_CTR_sink = new FlinkKafkaProducer010<Tuple3<Long, String, Double>>(KAFKA_SERVER, "email_CTR", new SerializationSchema<Tuple3<Long, String, Double>>() {
            @Override
            public byte[] serialize(Tuple3<Long, String, Double> longLongStringDoubleTuple4) {
                return longLongStringDoubleTuple4.toString().getBytes();
            }
        });

        FlinkKafkaProducer010<Tuple4<Long, Long, String, Double>> kafka_open_sink = new FlinkKafkaProducer010<Tuple4<Long, Long, String, Double>>(KAFKA_SERVER, "email_Openrate", new SerializationSchema<Tuple4<Long, Long, String, Double>>() {
            @Override
            public byte[] serialize(Tuple4<Long, Long, String, Double> longLongStringDoubleTuple4) {
                Long f0 = longLongStringDoubleTuple4.f0;
                Long f1 = longLongStringDoubleTuple4.f1;
                try {
                    Integer f0_int = f0.intValue();
                    Integer f1_int = f1.intValue();
                    return new Tuple4<>(f0_int, f1_int, longLongStringDoubleTuple4.f2, longLongStringDoubleTuple4.f3).toString().getBytes();
                } catch (Exception e) {
                    return longLongStringDoubleTuple4.toString().getBytes();
                }
            }
        });


        CTR.addSink(kafka_CTR_sink);
        openrate.addSink(kafka_open_sink);


        try {
            env.execute();
        } catch (Exception e) {
        }


    }
}
