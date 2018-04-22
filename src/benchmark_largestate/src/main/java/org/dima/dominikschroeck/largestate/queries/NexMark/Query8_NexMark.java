package org.dima.dominikschroeck.largestate.queries.NexMark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewPersonEvent;
import org.dima.dominikschroeck.largestate.KeySelectors.AuctionPersonKeySelector;
import org.dima.dominikschroeck.largestate.KeySelectors.PersonKeySelector;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query8_Person_Auction_JoinFunction;
import org.dima.dominikschroeck.largestate.queries.Query;

import java.util.Properties;

/**
 * NexMark Query 8: Window join n minutes: New users that opened an auction
 */
public class Query8_NexMark extends Query {

    StreamExecutionEnvironment ENV;
    Properties props;
    FlinkKafkaProducer010<String> producer;

    public Query8_NexMark(StreamExecutionEnvironment ENV, Properties props, FlinkKafkaProducer010<String> producer) {
        this.ENV = ENV;
        this.props = props;
        this.producer = producer;
    }

    /**
     * Join Operator for joining Auction Events and NewPerson events.
     *
     * @param openAuctions
     * @param NewPerson
     * @param windowSize   Size of Window in Time
     */
    public void invoke(DataStream<AuctionEvent> openAuctions, DataStream<NewPersonEvent> NewPerson, Time windowSize) {

        DataStream<Tuple4<Long, Integer, String, Long>> new_persons;

        /**
         * Window-Join for Persons and openAuctions.
         * Outputs every person that opened an auction within the time window
         */
        new_persons = NewPerson
                .join(openAuctions).where(new PersonKeySelector()).equalTo(new AuctionPersonKeySelector())
                .window(TumblingEventTimeWindows.of(windowSize))
                .apply(new Query8_Person_Auction_JoinFunction(ENV.getParallelism()))

        ;

        int join_parallelism = new_persons.getParallelism();


        /**
         * Sink and Query Latency
         */

        Integer query_para = props.getProperty("Sink_5_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_5_Parallelism"));

        /**
         * Using a Window operator here to find the latest ingestion timestamp.
         * Same Parallelism and Same Key as Join should make sure that Flink chains the operators
         */
        new_persons.keyBy(1).map(new MapFunction<Tuple4<Long,Integer,String,Long>, String>() {
            @Override
            public String map(Tuple4<Long, Integer, String, Long> longIntegerStringLongTuple4) throws Exception {
                return longIntegerStringLongTuple4.toString();
            }
        })
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query8_Sink");



        new_persons.keyBy(1) // Should allow chaining along with Join and reduce Latency! (Same key of previous window!)
                .window(TumblingEventTimeWindows.of(windowSize))
                .maxBy(3)
                .name("Query8_Metrics")
                .map(new RichMapFunction<Tuple4<Long, Integer, String, Long>, Tuple4<Long, Integer, String, Long>>() {

                    private Long latency = 0L;


                    @Override
                    public void open(Configuration parameters) {
                        try {
                            super.open(parameters);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        getRuntimeContext()
                                .getMetricGroup()
                                .gauge("Query8.OverallLatency", new Gauge<Long>() {
                                    @Override
                                    public Long getValue() {
                                        return latency;
                                    }
                                });
                    }

                    @Override
                    public Tuple4<Long, Integer, String, Long> map(Tuple4<Long, Integer, String, Long> longLongTuple2) {
                        this.latency = System.currentTimeMillis() - longLongTuple2.f3;

                        return longLongTuple2;
                    }
                })
                .setParallelism(join_parallelism) // In This way Flink should chain the operators!
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query8_ConvertMap")
                ;


    }
}
