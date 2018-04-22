package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;

/**
 * This CoFlatMap operator takes auctions and bids as input and stores a list of auction id's, their corresponding item id's and the auctions' end date. A maintenance function removes all expired auctions
 * This function outputs every item id that a bid was given on. We have to connect with auctions as a bid does not know which item it bids on, only the auctio
 */
public class Query5_Auction_to_Item_CoProcessFunction extends CoProcessFunction<BidEvent, AuctionEvent, Tuple3<Long, Integer, Long>> {

    private Long latency_1;
    private Long latency_2;


    // State stores auction_id, item_id, expiration timestamp

    private transient ValueState<Tuple3<Integer, Integer, Long>> auction_to_item;


    private Meter meter;
    private Long waitingTime_1, waitingTime_2, parallelism;

    public Query5_Auction_to_Item_CoProcessFunction(Integer parallelism) {


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
                .gauge("Query5_Auction_to_Item_CoProcessFunction.Latency_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_Auction_to_Item_CoProcessFunction.Latency_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_Auction_to_Item_CoProcessFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_Auction_to_Item_CoProcessFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_Auction_to_Item_CoProcessFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });
        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query5_Auction_to_Item_CoProcessFunction.Throughput", new DropwizardMeterWrapper(meter));


        ValueStateDescriptor<Tuple3<Integer, Integer, Long>> descriptor = new ValueStateDescriptor<Tuple3<Integer, Integer, Long>>(
                "Query5_Auction_to_Item_CoProcessFunction_state", TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>() {
        })
        );

        auction_to_item = getRuntimeContext().getState(descriptor);

        super.open(parameters);
    }


    /**
     * Emit an event after every bid. But only if ValueState is not empty. This way we avoid sending bids for auctions that expired
     *
     * @param bid
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement1(BidEvent bid, Context context, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
        // Whenever a bid arrives, return the itemid
        // Ignore bids for auctions that expired, i.e. not in the list
        Long start = System.currentTimeMillis();
        this.markEvent();
        this.waitingTime_1 = System.currentTimeMillis() - bid.getIngestion_timestamp();


        if (auction_to_item.value() != null) {
            Tuple3<Integer, Integer, Long> elem = auction_to_item.value();
            collector.collect(new Tuple3<>(System.currentTimeMillis(), elem.f1, bid.getIngestion_timestamp()));

        }

        this.latency_1 = System.currentTimeMillis() - start;
    }

    /**
     * Incoming auction. In a perfect world, we only receive one because we expect a keyed stream, keyed by auction ID's. And only one auction should be opened with the same ID
     * This method does not emit any event, just updates the respective valueState
     *
     * @param auction
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement2(AuctionEvent auction, Context context, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {


        Long start = System.currentTimeMillis();
        this.markEvent();

        this.waitingTime_2 = start - auction.getIngestion_timestamp();
        auction_to_item.update(new Tuple3<>(auction.getAuction_id(), auction.getItem_id(), auction.getEnd()));


        context.timerService().registerEventTimeTimer(auction.getEnd());
        this.latency_2 = System.currentTimeMillis() - start;
    }


    /**
     * Clear state on end of the auction
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Integer, Long>> out) throws Exception {

        if (auction_to_item.value() != null) {

            auction_to_item.clear();
        }


        super.onTimer(timestamp, ctx, out);
    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}