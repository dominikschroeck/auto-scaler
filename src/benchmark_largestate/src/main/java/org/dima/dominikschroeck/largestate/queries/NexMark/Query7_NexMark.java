package org.dima.dominikschroeck.largestate.queries.NexMark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query7_ActiveAuctions_Items_CoProcessFunction;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query7_AllWindowFunction;
import org.dima.dominikschroeck.largestate.queries.Query;

import java.util.Properties;

/**
 * NexMark Query 7: Items with highest bid (auction active in last 10 minutes)
 */
public class Query7_NexMark extends Query {

    StreamExecutionEnvironment ENV;
    Properties props;
    FlinkKafkaProducer010<String> producer;

    public Query7_NexMark(StreamExecutionEnvironment ENV, Properties props, FlinkKafkaProducer010<String> producer) {
        this.ENV = ENV;
        this.props = props;
        this.producer = producer;
    }

    public void invoke(DataStream<BidEvent> BidStream, DataStream<AuctionEvent> openAuctions, Time windowSize) {

        //FlinkKafkaProducer010<String> query7_sink = new FlinkKafkaProducer010<String>(props.getProperty("kafka_server"), "nexmark_results", new SimpleStringSchema());


        DataStream<Tuple4<Long, Integer, Double, Long>> highest_bid_in_10_minutes;


        /**
         * Join openAuctions and Bids to collect all bids for each auction
         */
        Integer query_para = props.getProperty("Query7_ActiveAuctions_Items_CoProcessFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query7_ActiveAuctions_Items_CoProcessFunction"));
        DataStream<Tuple5<Long, Integer, Double, Long, Long>> items_and_auctions = openAuctions
                .keyBy("auction_id")
                .connect(BidStream.keyBy("auction_id"))
                .process(new Query7_ActiveAuctions_Items_CoProcessFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query7_ActiveAuctions_Items_CoProcessFunction")
                .name("Query7_ActiveAuctions_Items_CoProcessFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Actual window. AllWindow (Non-Parallel), finds item(s) with highest price
         */
        highest_bid_in_10_minutes = items_and_auctions
                .windowAll(TumblingEventTimeWindows.of(windowSize))
                .apply(new Query7_AllWindowFunction(1))
                .uid("Query7_AllWindowFunction")
                .name("Query7_AllWindowFunction")
                .setMaxParallelism(1);



        /**
         * Sink and Query Latency
         */
        query_para = props.getProperty("Sink_4_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_4_Parallelism"));
        highest_bid_in_10_minutes
                .keyBy(1)
                .map(new MapFunction<Tuple4<Long,Integer,Double,Long>, String>() {

                    @Override
                    public String map(Tuple4<Long, Integer, Double, Long> longIntegerDoubleLongTuple4) throws Exception {
                        return "Query7," + longIntegerDoubleLongTuple4.toString();
                    }
                })
                .setParallelism(1) // In This way Flink should chain the operators!
                .setMaxParallelism(1)
                .name("Query7_ConvertMap")
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query7_Sink")
        ;


                /**.map(new RichMapFunction<Tuple4<Long, Integer, Double, Long>, String>() {

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
                        .gauge("Query7.OverallLatency", new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return latency;
                            }
                        });
            }

            @Override
            public String map(Tuple4<Long, Integer, Double, Long> longLongDoubleTuple3) {
                this.latency = System.currentTimeMillis() - longLongDoubleTuple3.f3;
                return "Query7," + longLongDoubleTuple3.toString();
            }
        })

                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query7_MetricsMap")
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query7_Sink");*/

    }
}
