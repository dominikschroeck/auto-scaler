package org.dima.dominikschroeck.largestate.functions.Email;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query2_AllWindowFunction extends RichWindowFunction<Tuple4<Long, String, String, String>, Tuple4<Long, Long, String, Double>, Tuple, TimeWindow> {
    private Meter meter;
    private Long waitingTime, latency;

    @Override
    public void open(Configuration parameters) throws Exception {
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query2_AllWindowFunction.Throughput", new DropwizardMeterWrapper(meter));

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query2_AllWindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query2_AllWindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, String, String, String>> iterable, Collector<Tuple4<Long, Long, String, Double>> collector) {
        Long start = System.currentTimeMillis();
        Long open_count = 0L;
        Long send_count = 0L;
        String campaign = iterable.iterator().next().f1;

        for (Tuple4<Long, String, String, String> event : iterable) {

            this.waitingTime = System.currentTimeMillis() - (timeWindow.getEnd() - timeWindow.getStart()) - event.f0;
            if (event.f3 == "OPEN") {
                open_count = open_count + 1;
            } else {
                send_count = send_count + 1;
            }

        }
        if (this.meter != null) {
            this.meter.markEvent(send_count + open_count);
        }
        collector.collect(new Tuple4<>(timeWindow.getStart(), timeWindow.getEnd(), campaign, Double.valueOf(open_count).doubleValue() / Double.valueOf(send_count).doubleValue()));
        this.latency = System.currentTimeMillis() - start;
    }
}
