package org.dima.dominikschroeck.largestate.queries.NexMark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query5_Auction_to_Item_CoProcessFunction;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query5_DetermineHottestItem_AllWindowFunction;
import org.dima.dominikschroeck.largestate.queries.Query;

import java.util.Properties;

/**
 * NexMark Query 5 Implementation of NexMark: "Hottest" Item within a time window.
 * Hottest: Most Bids (Count)
 */
public class Query5_NexMark extends Query {

    StreamExecutionEnvironment ENV;
    Properties props;
    FlinkKafkaProducer010<String> producer;

    public Query5_NexMark(StreamExecutionEnvironment ENV, Properties props, FlinkKafkaProducer010<String> producer) {
        this.ENV = ENV;
        this.props = props;
        this.producer = producer;
    }

    public void invoke(DataStream<BidEvent> BidStream, DataStream<AuctionEvent> openAuctions, Time windowSize, Time slide) {
        //FlinkKafkaProducer010<String> query5_sink = new FlinkKafkaProducer010<String>(props.getProperty("kafka_server"), "nexmark_results", new SimpleStringSchema());

        /**
         * CoProcess to join Bids with openAuctions
         * Reason: Need Item ID from Auction
         *
         */
        Integer query_para = props.getProperty("Query5_Auction_to_Item_CoProcessFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query5_Auction_to_Item_CoProcessFunction"));
        DataStream<Tuple3<Long, Integer, Long>> items_with_bid = BidStream.keyBy("auction_id")
                .connect(openAuctions.keyBy("auction_id")).process(new Query5_Auction_to_Item_CoProcessFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query5_Auction_to_Item_CoProcessFunction")
                .name("Query5_Auction_to_Item_CoProcessFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Actual window. Collects all Bids with Item id over a time window
         */
        DataStream<Tuple4<Long, Integer, Integer, Long>> hottest_item = items_with_bid
                .windowAll(SlidingEventTimeWindows.of(windowSize, slide))
                .apply(new Query5_DetermineHottestItem_AllWindowFunction(1))
                .uid("Query5_DetermineHottestItem_AllWindowFunction")
                .name("Query5_DetermineHottestItem_AllWindowFunction")
                .setMaxParallelism(1);


        /**
         * Sink and Query Latency
         */
        query_para = props.getProperty("Sink_2_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_2_Parallelism"));
        hottest_item.keyBy(1).map(new RichMapFunction<Tuple4<Long, Integer, Integer, Long>, String>() {

            @Override
            public String map(Tuple4<Long, Integer, Integer, Long> longLongTuple2) {
                return "Query5," + longLongTuple2.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query5_ConvertMap")
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query5_Sink");
    }
}
