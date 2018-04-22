package org.dima.dominikschroeck.largestate.queries.NexMark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent_Category;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewItemEvent;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query4_Auctions_Items_CoProcessFunction;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query4_AverageByCategory;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query4_Price_per_closed_Auction_CoProcessFunction;
import org.dima.dominikschroeck.largestate.queries.Query;

import java.util.Properties;

/**
 * NexMark Query 4: Average closing price per Item category
 */
public class Query4_NexMark extends Query {

    StreamExecutionEnvironment ENV;
    Properties props;
    FlinkKafkaProducer010<String> producer;


    public Query4_NexMark(StreamExecutionEnvironment ENV, Properties props, FlinkKafkaProducer010<String> producer) {
        this.ENV = ENV;
        this.props = props;
        this.producer = producer;
    }

    public void invoke(DataStream<AuctionEvent> openAuctions, DataStream<NewItemEvent> NewItems, DataStream<BidEvent> BidStream) {
        Integer query_para = 1;

        //FlinkKafkaProducer010<String> query4_sink = new FlinkKafkaProducer010<String>(props.getProperty("kafka_server"), "nexmark_results", new SimpleStringSchema());


        DataStream<Tuple4<Long, Integer, Double, Long>> average_price_per_category;


        /**
         * Connect Items and OpenAuctions
         * Extracting the Category from the Items into the Categories
         * 1 item : Many Auctions
         */
        query_para = props.getProperty("Query4_Auctions_Items_CoProcessFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query4_Auctions_Items_CoProcessFunction"));
        DataStream<AuctionEvent_Category> Category_Auctions = openAuctions.keyBy(" item_id")
                .connect(NewItems.keyBy(" item_id")).process(new Query4_Auctions_Items_CoProcessFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query4_Auctions_Items_CoProcessFunction")
                .name("Query4_Auctions_Items_CoProcessFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());

        /**
         * Join Category_Auctions with BidStream.
         * Collect all bids for an auction
         * Use onTimer of ProcessFunction to fire event on auction end.
         */
        query_para = props.getProperty("Query4_Price_per_closed_Auction_CoProcessFunction") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query4_Price_per_closed_Auction_CoProcessFunction"));
        DataStream<Tuple4<Long, Integer, Double, Long>> closed_auction_price_by_category = Category_Auctions.keyBy("auction_id")
                .connect(BidStream.keyBy("auction_id")).process(new Query4_Price_per_closed_Auction_CoProcessFunction(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query4_Price_per_closed_Auction_CoProcessFunction")
                .name("Query4_Price_per_closed_Auction_CoProcessFunction")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());

        /**
         * Compute the Average Final Price by Category
         * Use ValueState to Realize Continuous average.
         */
        query_para = props.getProperty("Query4_AverageByCategory") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Query4_AverageByCategory"));
        average_price_per_category = closed_auction_price_by_category
                .keyBy(1).map(new Query4_AverageByCategory(query_para == 0 ? ENV.getParallelism() : query_para))
                .uid("Query4_AverageByCategory")
                .name("Query4_AverageByCategory")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism());


        /**
         * Sinks and Overall Latency
         */
        query_para = props.getProperty("Sink_1_Parallelism") == null ? ENV.getParallelism() : Integer.parseInt(props.getProperty("Sink_1_Parallelism"));
        average_price_per_category.map(new RichMapFunction<Tuple4<Long, Integer, Double, Long>, String>() {

            @Override
            public String map(Tuple4<Long, Integer, Double, Long> longDoubleTuple2) {

                return "Query4," + longDoubleTuple2.toString();
            }
        })
                .name("Query4_ConvertMap")
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .setMaxParallelism(ENV.getMaxParallelism())
                .addSink(producer)
                .setParallelism(query_para == 0 ? ENV.getParallelism() : query_para)
                .name("Query4_Sink");


    }
}
