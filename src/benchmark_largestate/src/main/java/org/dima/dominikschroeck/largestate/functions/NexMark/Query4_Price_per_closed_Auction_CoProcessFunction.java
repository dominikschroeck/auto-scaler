package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent_Category;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;


/**
 * This function receives an auction and n bids for this auction. Both incoming streams are keyed by auction_id. The price for an auction will be updated as long bids are incoming
 * and the auction has not expired (end timestamp!)
 */
public class Query4_Price_per_closed_Auction_CoProcessFunction extends CoProcessFunction<AuctionEvent_Category, BidEvent, Tuple4<Long, Integer, Double, Long>> {

    private Long latency_1;
    private Long latency_2;
    private Long waitingTime_1, waitingTime_2;
    private Meter meter;
    private transient ValueState<AuctionEvent_Category> auction_state;
    private transient ValueState<Tuple2<Double, Long>> bid_state;
    private Long parallelism;


    public Query4_Price_per_closed_Auction_CoProcessFunction(Integer parallelism) {
        this.latency_1 = 0L;
        this.waitingTime_2 = 0L;
        this.waitingTime_1 = 0L;
        this.parallelism = parallelism.longValue();


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Price_per_closed_Auction_CoProcessFunction.Latency_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Price_per_closed_Auction_CoProcessFunction.Latency_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_2;
                    }
                });

        // Waiting Time
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Price_per_closed_Auction_CoProcessFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Price_per_closed_Auction_CoProcessFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Price_per_closed_Auction_CoProcessFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });



        // Throughput

        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query4_Price_per_closed_Auction_CoProcessFunction.Throughput", new DropwizardMeterWrapper(meter));


        // Throughput
        com.codahale.metrics.Meter meter_2 = new com.codahale.metrics.Meter();

        /*this.meter_2 = getRuntimeContext ( )
                .getMetricGroup ( )
                .meter ( "Query4_Price_per_closed_Auction_CoProcessFunction.Throughput_2", new DropwizardMeterWrapper ( meter_2 ) );
*/

        // 8 +4 +8 + 8 + 4 + 8
        ValueStateDescriptor<AuctionEvent_Category> descriptor = new ValueStateDescriptor<>(
                "Query4_Price_per_closed_Auction_CoProcessFunction.auction_ID_state",
                TypeInformation.of(new TypeHint<AuctionEvent_Category>() {
                })
        );

        // 16 bytes
        ValueStateDescriptor<Tuple2<Double, Long>> bid_descriptor = new ValueStateDescriptor<Tuple2<Double, Long>>(
                "Query4_Price_per_closed_Auction_CoProcessFunction.bid_state",
                TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {
                })
        );


        auction_state = getRuntimeContext().getState(descriptor);

        bid_state = getRuntimeContext().getState(bid_descriptor);

        super.open(parameters);
    }


    /**
     * Receiving AuctionEvents with Category and register a timer for the auction's ending time. Stores the auction in a ValueState in order to be able to update bid prices
     *
     * @param input
     * @param context
     * @param collector This method only collects the auction, does not fire any event
     * @throws Exception
     */
    @Override
    public void processElement1(AuctionEvent_Category input, Context context, Collector<Tuple4<Long, Integer, Double, Long>> collector) throws Exception {
        Long start = System.currentTimeMillis();
        this.waitingTime_1 = start - input.timestamp;
        this.markEvent();

        auction_state.update(input);

        context.timerService().registerEventTimeTimer(input.end);

        this.latency_1 = System.currentTimeMillis() - start;
    }


    /**
     * Receives Bid Events and updates the bid price of its own bid state
     * As auction state may still be empty, collect the latest bid price in a state. Always stores the highest value
     *
     * @param bid
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement2(BidEvent bid, Context context, Collector<Tuple4<Long, Integer, Double, Long>> collector) throws Exception {

        Long start = System.currentTimeMillis();
        this.markEvent();


        this.waitingTime_2 = start - bid.ingestion_timestamp;


        if (this.bid_state.value() != null) {
            Tuple2<Double, Long> current_bid_state = this.bid_state.value();
            Long timestamp = current_bid_state.f1;
            Double price = current_bid_state.f0;

            if (timestamp < bid.ingestion_timestamp)
                timestamp = bid.ingestion_timestamp;
            if (price < bid.bid)
                price = bid.bid;

            this.bid_state.update(Tuple2.of(price, timestamp));

        } else {
            this.bid_state.update(Tuple2.of(bid.bid, bid.ingestion_timestamp));

        }


        this.latency_2 = System.currentTimeMillis() - start;

    }

    /**
     * On end of an auction, fire the auction with the final bid price. Clears state
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Integer, Double, Long>> out) throws Exception {

        // Read state
        AuctionEvent_Category state = new AuctionEvent_Category();
        Tuple2<Double, Long> current_bid_state;
        Long ingestion_stamp = new Long(0);
        Double price = 0D;

        if (bid_state.value() != null && auction_state.value() != null) {
            state = auction_state.value();
            current_bid_state = this.bid_state.value();
            price = current_bid_state.f0;
            if (current_bid_state.f1 > state.ingestion_timestamp) ingestion_stamp = current_bid_state.f1;
            else ingestion_stamp = state.ingestion_timestamp;
        }

        if (bid_state.value() == null && auction_state.value() != null) {
            state = auction_state.value();
            price = state.intialPrice;
            ingestion_stamp = state.ingestion_timestamp;
        }


        if (auction_state.value() != null) {
            out.collect(new Tuple4<>(System.currentTimeMillis(), state.category_id, price, ingestion_stamp));

        }
        super.onTimer(timestamp, ctx, out);

        // Clear states

        auction_state.clear();

        bid_state.clear();



    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}
