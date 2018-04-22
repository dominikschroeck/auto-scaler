package org.dima.dominikschroeck.largestate.functions.NexMark;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;


/**
 * This CoProcess operator takes auctions and bids as input and stores a list of auction id's, their corresponding item id's and the auctions' end date.
 * This function outputs every seller's closing price when an auction is closed
 */
public class Query6_Auctions_to_Bids_CoProcessFunction extends CoProcessFunction<BidEvent, AuctionEvent, Tuple4<Long, Integer, Double, Long>> {
    // State stores auction_id, item_id, expiration timestamp
    private Long maintenance_interval = 10000L;

    private Long latency_1;
    private Long latency_2;
    private Long waitingTime_1, waitingTime_2, parallelism;
    private Meter meter;
    private transient ValueState<Tuple5<Integer, Integer, Long, Double, Long>> auction_to_seller_state;
    private transient ValueState<Tuple2<Double, Long>> bid_state;


    public Query6_Auctions_to_Bids_CoProcessFunction(Integer parallelism) {


        this.latency_1 = 0L;
        this.latency_2 = 0L;
        this.waitingTime_2 = 0L;
        this.waitingTime_1 = 0L;
        this.parallelism = parallelism.longValue();

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_Auctions_to_Bids_CoProcessFunction.Latency_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_Auctions_to_Bids_CoProcessFunction.Latency_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_Auctions_to_Bids_CoProcessFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_Auctions_to_Bids_CoProcessFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_Auctions_to_Bids_CoProcessFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });

        // Throughput
        com.codahale.metrics.Meter codahale_meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query6_Auctions_to_Bids_CoProcessFunction.Throughput", new DropwizardMeterWrapper(codahale_meter));


        ValueStateDescriptor<Tuple5<Integer, Integer, Long, Double, Long>> descriptor =
                new ValueStateDescriptor<Tuple5<Integer, Integer, Long, Double, Long>>(
                        "Query6_Auctions_to_Bids_CoProcessFunction.auctionState", TypeInformation.of(new TypeHint<Tuple5<Integer, Integer, Long, Double, Long>>() {
                })
                );

        ValueStateDescriptor<Tuple2<Double, Long>> bid_descriptor =
                new ValueStateDescriptor<Tuple2<Double, Long>>(
                        "Query6_Auctions_to_Bids_CoProcessFunction.bidState",
                        TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {
                        })
                );


        bid_state = getRuntimeContext().getState(bid_descriptor);

        auction_to_seller_state = getRuntimeContext().getState(descriptor);

        super.open(parameters);
    }


    /**
     * Receives Bid Events. Updates the price for the bid ValueState (Decoupling auctions and bids in case of late arrival of one of them)
     *
     * @param bid
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement1(BidEvent bid, Context context, Collector<Tuple4<Long, Integer, Double, Long>> collector) throws Exception {
        // Whenever a bid arrives, update the price in the valuestate
        // Ignore bids for auctions that expired, i.e. not in the list

        Long start = System.currentTimeMillis();
        this.waitingTime_1 = start - bid.getIngestion_timestamp();
        this.markEvent();


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


        this.latency_1 = System.currentTimeMillis() - start;

    }

    /**
     * Stores the auction in the ValueState. We should only receive one auction and many bids. Setting a timer to the expiration date of the auction.
     *
     * @param auction
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement2(AuctionEvent auction, Context context, Collector<Tuple4<Long, Integer, Double, Long>> collector) throws Exception {


        Long start = System.currentTimeMillis();
        this.waitingTime_2 = start - auction.getIngestion_timestamp();
        this.markEvent();


        auction_to_seller_state.update(new Tuple5<>(auction.auction_id, auction.person_id, auction.getEnd(), auction.getIntialPrice(), auction.getIngestion_timestamp()));
        context.timerService().registerEventTimeTimer(auction.getEnd());
        this.latency_2 = System.currentTimeMillis() - start;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Integer, Double, Long>> out) throws Exception {

        Tuple5<Integer, Integer, Long, Double, Long> auction_seller = new Tuple5<>();
        Tuple2<Double, Long> current_bid_state = new Tuple2<>();
        Long ingestion_stamp = new Long(0);
        Double price = 0D;
        if (bid_state.value() != null && auction_to_seller_state.value() != null) {
            auction_seller = auction_to_seller_state.value();
            current_bid_state = this.bid_state.value();
            price = current_bid_state.f0;

            if (current_bid_state.f1 > auction_seller.f4) ingestion_stamp = current_bid_state.f1;
            else ingestion_stamp = auction_seller.f4;

            out.collect(new Tuple4<>(System.currentTimeMillis(), auction_seller.f1, price, ingestion_stamp));
        }

        /**
         * In case we did not receive any bid
         */
        if (bid_state.value() == null && auction_to_seller_state.value() != null) {
            auction_seller = auction_to_seller_state.value();
            price = auction_seller.f3;
            ingestion_stamp = auction_seller.f4;
            auction_seller = auction_to_seller_state.value();
            out.collect(new Tuple4<>(System.currentTimeMillis(), auction_seller.f1, price, ingestion_stamp));
        }


        // Clear State

        auction_to_seller_state.clear();

        super.onTimer(timestamp, ctx, out);

    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}