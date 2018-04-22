package org.dima.dominikschroeck.largestate.queries.NexMark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query6_Auctions_to_Bids_CoProcessFunction;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query6_AverageSellerPrice_WindowFunction;
import org.dima.dominikschroeck.largestate.queries.Query;

import java.util.Properties;

/*
 * NexMark Query 6: Seller average price for last n auctions (only closed auctions)
 */
public class Query6_NexMark extends Query {

    StreamExecutionEnvironment ENV;
    Properties props;
    FlinkKafkaProducer010<String> producer;

    public Query6_NexMark(StreamExecutionEnvironment ENV, Properties props, FlinkKafkaProducer010<String> producer) {
        this.ENV = ENV;
        this.props = props;
        this.producer = producer;
    }

    public void invoke(DataStream<BidEvent> BidStream, DataStream<AuctionEvent> openAuctions, Integer windowSize, Integer slide) {


        /**
         * Connect OpenAuctions with the bids. Outputs every closed auction along with the final price and seller ID
         * (Using onTimer EventTime trigger)
         */
        Integer query_para = props.getProperty("Query6_Auctions_to_Bids_CoProcessFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query6_Auctions_to_Bids_CoProcessFunction"));
        DataStream<Tuple4<Long, Integer, Double, Long>> closed_auctions_and_sellers = BidStream.keyBy("auction_id")
                .connect(openAuctions.keyBy("auction_id")).process(new Query6_Auctions_to_Bids_CoProcessFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query6_Auctions_to_Bids_CoProcessFunction")
                .name("Query6_Auctions_to_Bids_CoProcessFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());

        /**
         * Key by seller (person) ID and compute actual Window for each seller
         */
        query_para = props.getProperty("Query6_AverageSellerPrice_WindowFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query6_AverageSellerPrice_WindowFunction"));
        DataStream<Tuple4<Long, Integer, Double, Long>> average_price_by_seller = closed_auctions_and_sellers
                .keyBy(1)
                .countWindow(windowSize, slide)
                .apply(new Query6_AverageSellerPrice_WindowFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query6_AverageSellerPrice_WindowFunction")
                .name("Query6_AverageSellerPrice_WindowFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Sink and Query Latency
         */
        query_para = props.getProperty("Sink_3_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_3_Parallelism"));
        average_price_by_seller.keyBy(1).map(new RichMapFunction<Tuple4<Long, Integer, Double, Long>, String>() {


            @Override
            public String map(Tuple4<Long, Integer, Double, Long> longLongDoubleTuple3) {
                return "Query6," + longLongDoubleTuple3.toString();
            }
        })
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .name("Query6_ConvertMap")
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query6_Sink");
    }
}
