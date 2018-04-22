package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This WindowAll function finds the item with the most bids over the window. Receives a Stream with item ids. These events always occur on a bid on an item
 */
public class Query5_DetermineHottestItem_AllWindowFunction extends RichAllWindowFunction<Tuple3<Long, Integer, Long>, Tuple4<Long, Integer, Integer, Long>, TimeWindow> {
    private Long latency, overall_latency;
    private Meter meter;
    private Long waitingTime;
    private Long parallelism;

    public Query5_DetermineHottestItem_AllWindowFunction(Integer parallelism) {
        this.latency = 0L;
        this.waitingTime = 0L;
        this.overall_latency = 0L;
        this.parallelism = parallelism.longValue();

    }

    @Override
    public void open(Configuration parameters) throws Exception {

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_DetermineHottestItem_AllWindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_DetermineHottestItem_AllWindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_DetermineHottestItem_AllWindowFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });

        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query5_DetermineHottestItem_AllWindowFunction.Throughput", new DropwizardMeterWrapper(meter));

        com.codahale.metrics.Meter meter_2 = new com.codahale.metrics.Meter();






        super.open(parameters);
    }

    /**
     * Returns the count for the item with maximal count
     *
     * @param timeWindow
     * @param iterable
     * @param collector
     * @throws Exception
     */
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Long>> iterable, Collector<Tuple4<Long, Integer, Integer, Long>> collector) {

        Long start = System.currentTimeMillis();
        Long latest_stamp = 0L;

        HashMap<Integer, Integer> count_map = new HashMap<Integer, Integer>();
        int currentHighest_id = 0;
        int currentHighest = 0;
        long count = 0;
        Long input_stamp = 0L;

        for (Iterator<Tuple3<Long, Integer, Long>> iter = iterable.iterator(); iter.hasNext(); ) {
            count += 1;
            Tuple3<Long, Integer, Long> elem = iter.next();
            this.markEvent();

            if (elem.f2 > latest_stamp) {
                latest_stamp = elem.f2;
            }
            Integer id = elem.f1;

            // Looking for the highest input stamp to compute waiting time

            if (elem.f0 > input_stamp) input_stamp = elem.f0;


            if (count_map.containsKey(id)) {
                int id_count = count_map.get(id) + 1;
                count_map.remove(id);
                count_map.put(id, id_count);
                if (id_count > currentHighest) {
                    currentHighest_id = id;
                    currentHighest = id_count;
                }
            } else {
                count_map.put(id, 1);
            }
        }


        /**
         * Iterate through list and find all with the highest count and emit them
         */
        for (Map.Entry<Integer, Integer> entry : count_map.entrySet()) {
            if (entry.getValue() == currentHighest)
                collector.collect(new Tuple4<>(System.currentTimeMillis(), entry.getKey(), currentHighest, latest_stamp));
        }

        this.waitingTime = start - input_stamp;


        //collector.collect ( new Tuple5<> ( System.currentTimeMillis ( ), currentHighest_id, currentHighest, latest_stamp, throughput ) );
        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - latest_stamp;
    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}
