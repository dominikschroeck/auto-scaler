package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent_Category;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewItemEvent;

import java.util.Iterator;

/**
 * CoProcess function for Query 4. Connects AuctionEvents and ItemEvents. It collects one Item from the item stream and many auctions. Streams keyed by Item_id.
 * Enriches the AuctionEvent Stream with the Category_id which is only visible in the ItemEvents.
 */
public class Query4_Auctions_Items_CoProcessFunction extends CoProcessFunction<AuctionEvent, NewItemEvent, AuctionEvent_Category> {

    ListState<AuctionEvent_Category> waiting_for_category;
    private Long latency_1, latency_2;
    private Long waitingTime_1;
    private Long waitingTime_2;
    private Meter meter;
    private Long parallelism;



    private transient ValueState<Integer> category;

    public Query4_Auctions_Items_CoProcessFunction(Integer parallelism) {
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
                .gauge("Query4_Auctions_Items_CoProcessFunction.Latency_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Auctions_Items_CoProcessFunction.Latency_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Auctions_Items_CoProcessFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Auctions_Items_CoProcessFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_Auctions_Items_CoProcessFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });




        com.codahale.metrics.Meter meter_1 = new com.codahale.metrics.Meter();


        this.meter =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter("Query4_Auctions_Items_CoProcessFunction.Throughput", new DropwizardMeterWrapper(meter_1));


        ListStateDescriptor<AuctionEvent_Category> descriptor =
                new ListStateDescriptor<>(
                        "Query4_Auctions_Items_CoProcessFunction_state",
                        TypeInformation.of(new TypeHint<AuctionEvent_Category>() {
                        }));

        waiting_for_category = getRuntimeContext().getListState(descriptor);

        ValueStateDescriptor<Integer> category_descriptor =
                new ValueStateDescriptor<>("Query4_Auctions_Items_CoProcessFunction_CategoryState", TypeInformation.of(new TypeHint<Integer>() {
                }));

        this.category = getRuntimeContext().getState(category_descriptor);

        super.open(parameters);
    }

    /**
     * Receiving AuctionEvents. Either immediately fire AuctionEvent with Category or put into a ListState if the respective item has not yet been received
     *
     * @param input
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement1(AuctionEvent input, Context context, Collector<AuctionEvent_Category> collector) throws Exception {
        Long start = System.currentTimeMillis();

        this.waitingTime_1 = start - input.getIngestion_timestamp();

        this.markEvent();


        if (this.category.value() != null) {

            collector.collect(new AuctionEvent_Category(System.currentTimeMillis(), input.getAuction_id(), input.getIntialPrice(), input.getEnd(), this.category.value(), input.getIngestion_timestamp()));


        } else {
            waiting_for_category.add(new AuctionEvent_Category(System.currentTimeMillis(), input.getAuction_id(), input.getIntialPrice(), input.getEnd(), 0, input.getIngestion_timestamp()));

        }

        this.latency_1 = System.currentTimeMillis() - start;

        context.timerService().registerEventTimeTimer(input.end);


    }

    /**
     * When an item arrives, store the category in ValueState and emit the auctions already in the ListState ("Waiting" for a Category)
     *
     * @param newitem
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement2(NewItemEvent newitem, Context context, Collector<AuctionEvent_Category> collector) throws Exception {
        Long start = System.currentTimeMillis();


        this.waitingTime_2 = start - newitem.getIngestion_timestamp();

        this.category.update(newitem.getCategory_id());

        this.latency_2 = System.currentTimeMillis() - start;

        //context.timerService ( ).registerEventTimeTimer ( context.timestamp ( ) + 10000L );

        /**
         * Now we have an item and can start emitting the auctions along with categories
         */
        for (Iterator<AuctionEvent_Category> iter = waiting_for_category.get().iterator(); iter.hasNext(); ) {
            AuctionEvent_Category input = iter.next();
            this.markEvent();


            collector.collect(new AuctionEvent_Category(System.currentTimeMillis(), input.getAuction_id(), input.getIntialPrice(), input.getEnd(), this.category.value(), input.getIngestion_timestamp()));

            iter.remove();

        }
        waiting_for_category.clear();


    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AuctionEvent_Category> collector) throws Exception {
        if (this.category.value() != null){
        for (Iterator<AuctionEvent_Category> iter = waiting_for_category.get().iterator(); iter.hasNext(); ) {
            AuctionEvent_Category input = iter.next();
            this.markEvent();


            collector.collect(new AuctionEvent_Category(System.currentTimeMillis(), input.getAuction_id(), input.getIntialPrice(), input.getEnd(), this.category.value(), input.getIngestion_timestamp()));

            iter.remove();

        }
        waiting_for_category.clear();}
    }
}
