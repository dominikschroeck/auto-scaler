package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Query4_RichWindowFunction extends RichWindowFunction<Tuple6<Long, String, String, String, String, Double>, Tuple4<Long, Long, String, Double>, Tuple, TimeWindow> {

    private Long latency = 0L;
    private Long waitingTime = 0L;
    private Meter meter;
    private Long overall_latency = 0L;
    private Long parallelism = 0L;

    public Query4_RichWindowFunction(Integer parallelism) {
        this.parallelism = parallelism.longValue();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_RichWindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_RichWindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_RichWindowFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query4_RichWindowFunction.Throughput", new DropwizardMeterWrapper(meter));
        super.open(parameters);
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Long, String, String, String, String, Double>> iterable, Collector<Tuple4<Long, Long, String, Double>> collector) {
        Long start = System.currentTimeMillis();
        String key = tuple.getField(0);
        Long impression_count = 0L;
        Long click_count = 0L;
        Long input_stamp = 0L;

        for (Iterator<Tuple6<Long, String, String, String, String, Double>> iter = iterable.iterator(); iter.hasNext(); ) {
            Tuple6<Long, String, String, String, String, Double> elem = iter.next();

            if (elem.f0 > input_stamp) input_stamp = elem.f0;
            //this.waitingTime = System.currentTimeMillis ( ) - (timeWindow.getStart ( ) - timeWindow.getEnd ( )) - elem.f0;
            if (elem.f1.equals("CLICK")) {
                click_count += 1;
            } else if (elem.f1.equals("IMPRESSION")) {
                impression_count += 1;
            }

            meter.markEvent();

        }

        collector.collect(new Tuple4<>(timeWindow.getStart(), timeWindow.getEnd(), key, Double.valueOf(impression_count) / Double.valueOf(click_count)));
        this.waitingTime = start - input_stamp;
        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - input_stamp;

    }
}
