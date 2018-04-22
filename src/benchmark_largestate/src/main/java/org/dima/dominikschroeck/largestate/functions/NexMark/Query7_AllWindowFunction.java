package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Window Function that computes the items with the highest bids
 */
public class Query7_AllWindowFunction extends RichAllWindowFunction<Tuple5<Long, Integer, Double, Long, Long>, Tuple4<Long, Integer, Double, Long>, TimeWindow> {
    private Long latency, overall_latency;
    private Meter meter;
    private Long waitingTime, parallelism;

    public Query7_AllWindowFunction(Integer parallelism) {
        this.latency = 0L;
        this.waitingTime = 0L;
        this.overall_latency = 0L;
        this.parallelism = parallelism.longValue();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_AllWindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_AllWindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query7_AllWindowFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });



        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query7_AllWindowFunction.Throughput", new DropwizardMeterWrapper(meter));

        super.open(parameters);


    }

    /**
     * Finds the highest bid and all events that store this particular bid and emits them.
     *
     * @param window
     * @param values
     * @param out
     * @throws Exception
     */
    @Override
    public void apply(TimeWindow window, Iterable<Tuple5<Long, Integer, Double, Long, Long>> values, Collector<Tuple4<Long, Integer, Double, Long>> out) {
        Long start = System.currentTimeMillis();
        Long highest_timestamp = 0L;


        Tuple5<Long, Integer, Double, Long, Integer> max = new Tuple5<>(0L, 0, 0D, 0L, 0);
        Double highest_bid = 0D;

        Long input_stamp = 0L;
        Long count = 0L;
        /**
         * Find the highest bid
         */
        for (Iterator<Tuple5<Long, Integer, Double, Long, Long>> iter = values.iterator(); iter.hasNext(); ) {
            Tuple5<Long, Integer, Double, Long, Long> elem = iter.next();
            if (elem.f3 > highest_timestamp) {
                highest_timestamp = elem.f3;
            }
            count = count + 1;

            // Finding latest timestamp of incoming event for Waiting Time measure
            // This is the timestamp when the event left the previous operator
            if (elem.f0 > input_stamp) input_stamp = elem.f0;


            if (elem.f2 > highest_bid) {
                highest_bid = elem.f2;
            }
        }



        /**
         * Find all elements with this highest bid
         */

        for (Iterator<Tuple5<Long, Integer, Double, Long, Long>> iter = values.iterator(); iter.hasNext(); ) {
            Tuple5<Long, Integer, Double, Long, Long> elem = iter.next();
            if (elem.f2 == highest_bid)
                out.collect(new Tuple4<>(elem.f0, elem.f1, highest_bid, highest_timestamp));
            this.markEvent();
        }



        this.waitingTime = start - input_stamp;

        this.overall_latency = System.currentTimeMillis() - highest_timestamp;

        this.latency = System.currentTimeMillis() - start;
    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}
