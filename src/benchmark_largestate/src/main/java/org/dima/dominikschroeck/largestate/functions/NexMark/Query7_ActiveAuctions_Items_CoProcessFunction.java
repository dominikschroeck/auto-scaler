package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * This class receives one openAuction event and many bids as input and returns all bids for the active auctions. This class is required to match auctions to items.
 * It only returns results if the auction has not expired when the bid comes in.
 * The output is a Tuple3 of timestamp,item_id, bid_price
 */
public class Query7_ActiveAuctions_Items_CoProcessFunction extends CoProcessFunction<AuctionEvent, BidEvent,
        Tuple5<Long, Integer, Double, Long, Long>> {

    /**
     * Auctions: Auction_ID, Item_id,End, CurrentPrice
     */
    //private ArrayList<Tuple4<Long,Long,Long,Double>> auctions;
    private Long maintenance_interval = 10000L;
    // ListState: EventTime timestamp, Price , ingestion_timestamp
    private ListState<Tuple3<Long, Double, Long>> bid_state;
    private Long latency_1, latency_2;
    private Meter meter;
    private Long waitingTime_1, waitingTime_2,parallelism;


    private transient ValueState<Tuple4<Integer, Integer, Long, Double>> auction_state;

    public Query7_ActiveAuctions_Items_CoProcessFunction(Integer parallelism) {
        this.waitingTime_2 = 0L;
        this.waitingTime_1 = 0L;
        this.parallelism = parallelism.longValue();


    }


    @Override
    public void open(Configuration parameters) throws Exception {

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_ActiveAuctions_Items_CoProcessFunction.Latency_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_1;
                    }
                });
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_ActiveAuctions_Items_CoProcessFunction.Latency_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_ActiveAuctions_Items_CoProcessFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_ActiveAuctions_Items_CoProcessFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_ActiveAuctions_Items_CoProcessFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });



        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query7_ActiveAuctions_Items_CoProcessFunction.Throughput", new DropwizardMeterWrapper(meter));


        ValueStateDescriptor<Tuple4<Integer, Integer, Long, Double>> descriptor = new ValueStateDescriptor<Tuple4<Integer, Integer, Long, Double>>(
                "Query7_ActiveAuctions_Items_CoProcessFunction.auction_state",
                TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Long, Double>>() {
                })
        );

        ListStateDescriptor<Tuple3<Long, Double, Long>> bid_descriptor = new ListStateDescriptor<Tuple3<Long, Double, Long>>(
                "Query7_ActiveAuctions_Items_CoProcessFunction.bid_state",
                TypeInformation.of(new TypeHint<Tuple3<Long, Double, Long>>() {
                }));


        bid_state = getRuntimeContext().getListState(bid_descriptor);

        auction_state = getRuntimeContext().getState(descriptor);

        super.open(parameters);
    }

    /**
     * Incoming auction: timestamp + "," + auction_id + "," + person_id + "," + item_id + "," + initialPrice + "," +  reserve + "," + start + "," + end
     * Does not send any elements downstream!
     *
     * @param auction
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement1(AuctionEvent auction, Context context, Collector<Tuple5<Long, Integer, Double, Long, Long>> collector) throws Exception {
        Long start = System.currentTimeMillis();
        this.markEvent();


        this.waitingTime_1 = start - auction.getIngestion_timestamp();
        Integer auction_id = auction.getAuction_id();
        Integer item_id = auction.getItem_id();
        Double currentPrice = auction.getIntialPrice();
        Long auction_end = auction.getEnd();

        this.auction_state.update(new Tuple4<>(auction_id, item_id, auction_end, currentPrice));

        /**
         * Timer: When auction ends, clear state and end!
         */
        context.timerService().registerEventTimeTimer(auction_end);


        for (Tuple3<Long, Double, Long> bid : bid_state.get()) {

            collector.collect(new Tuple5<>(System.currentTimeMillis(), item_id, bid.f1, bid.f2, bid.f0));
            bid_state.clear();
        }


        // No output!
        this.latency_1 = System.currentTimeMillis() - start;
    }

    /**
     * Handles incoming bids
     * Will update the current active auction's prices the bid refers to. Additionally, collect the bid,item_id and the bid_price
     *
     * @param bid
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement2(BidEvent bid, Context context, Collector<Tuple5<Long, Integer, Double, Long, Long>> collector) throws Exception {
        Long start = System.currentTimeMillis();

        this.markEvent();

        if (auction_state.value() != null) {


            this.waitingTime_2 = start - bid.getIngestion_timestamp();
            Double price = bid.getBid();


            Tuple4<Integer, Integer, Long, Double> auction = auction_state.value();
            Integer item_id = auction.f1;
            collector.collect(new Tuple5<>(System.currentTimeMillis(), item_id, price, bid.getIngestion_timestamp(), bid.timestamp));


        } else {
            // If auction not yet received, store bin in state for later processing in ProcessElement1
            bid_state.add(new Tuple3<>(bid.timestamp, bid.bid, bid.ingestion_timestamp));
        }
        this.latency_2 = System.currentTimeMillis() - start;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple5<Long, Integer, Double, Long, Long>> collector) throws Exception {


        auction_state.clear();


        super.onTimer(timestamp, ctx, collector);
    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}
