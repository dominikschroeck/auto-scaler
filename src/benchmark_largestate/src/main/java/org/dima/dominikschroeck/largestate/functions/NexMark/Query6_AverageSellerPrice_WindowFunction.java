package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * This function computes the Average Seller Price on a Stream keyed by Seller and the final price for each auction
 */
public class Query6_AverageSellerPrice_WindowFunction extends RichWindowFunction<Tuple4<Long, Integer, Double, Long>, Tuple4<Long, Integer, Double, Long>, Tuple, GlobalWindow> {

    private Long latency,overall_latency;
    private Meter meter;
    private Long waitingTime, parallelism;

    public Query6_AverageSellerPrice_WindowFunction(Integer parallelism) {
        this.latency = 0L;
        this.waitingTime = 0L;
        this.overall_latency = 0L;
        this.parallelism = parallelism.longValue();
    }

    @Override
    public void open(Configuration parameters) throws Exception {


        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_AverageSellerPrice_WindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_AverageSellerPrice_WindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query6_AverageSellerPrice_WindowFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query6_AverageSellerPrice_WindowFunction.Throughput", new DropwizardMeterWrapper(meter));

        super.open(parameters);
    }

    /**
     * Applying the WindowFunction. Computes a sum of the closing prices and counts the number of events, returns the average along with Seller ID
     *
     * @param tuple
     * @param globalWindow
     * @param iterable
     * @param collector
     * @throws Exception
     */
    @Override
    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple4<Long, Integer, Double, Long>> iterable, Collector<Tuple4<Long, Integer, Double, Long>> collector) {
        Long start = System.currentTimeMillis();
        Long highest_timestamp = 0L;


        Double currentSum = 0D;
        Integer seller = iterable.iterator().next().f1;
        long count = 0;

        Long input_stamp = 0L;


        for (Iterator<Tuple4<Long, Integer, Double, Long>> iter = iterable.iterator(); iter.hasNext(); ) {
            Tuple4<Long, Integer, Double, Long> seller_price = iter.next();


            if (seller_price.f3 > highest_timestamp) {
                highest_timestamp = seller_price.f3;
            }

            if (seller_price.f0 > input_stamp) input_stamp = seller_price.f0;

            count = count + 1L;
            currentSum = currentSum + seller_price.f2;
            this.markEvent();

        }


        collector.collect(new Tuple4<>(System.currentTimeMillis(), seller, Double.valueOf(currentSum).doubleValue() / Double.valueOf(count).doubleValue(), highest_timestamp));
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
