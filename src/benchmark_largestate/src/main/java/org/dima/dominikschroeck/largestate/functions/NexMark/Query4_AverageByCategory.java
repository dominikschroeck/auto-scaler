package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.functions.RichMapFunction;
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


/**
 * Computes the average price per category. Receive a stream keyed by Category with a price field
 */
public class Query4_AverageByCategory extends RichMapFunction<Tuple4<Long, Integer, Double, Long>, Tuple4<Long, Integer, Double, Long>> {
    ValueState<Tuple2<Long, Double>> average;
    private Long latency, overall_latency;
    private Meter meter;
    private Long waitingTime, parallelism;

    public Query4_AverageByCategory(Integer parallelism) {
        this.latency = 0L;
        this.waitingTime = 0L;
        this.overall_latency = 0L;
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

        ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
                new ValueStateDescriptor<>(
                        "Query4_average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
                        })
                );

        average = getRuntimeContext().getState(descriptor);

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_AverageByCategory.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });


        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_AverageByCategory.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_AverageByCategory.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });

        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext().getMetricGroup().meter("Query4_AverageByCategory.Throughput", new DropwizardMeterWrapper(meter));


        super.open(parameters);

    }

    /**
     * Receive event and compute average using a ValueState for a running price sum and count
     *
     * @param input Tuple5 of: Timestamp (Leaving previous operator), Category ID, Closing Price, Ingestion Stamp, Throughput
     * @return Tuple 5 of: Timestamp (now), Category ID, Average (sum / count), Ingestion Stamp, Throughput (unchanged)
     * @throws Exception
     */
    @Override
    public Tuple4<Long, Integer, Double, Long> map(Tuple4<Long, Integer, Double, Long> input) throws Exception {
        Long start = System.currentTimeMillis();
        this.markEvent();

        this.waitingTime = start - input.f0;
        long count = 1L;
        double sum = input.f2.doubleValue();
        if (average.value() != null) {
            count = 1L + average.value().f0.longValue();
            sum = average.value().f1.doubleValue() + input.f2.doubleValue();
        }

        average.update(new Tuple2<>(count, sum));

        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - input.f3;
        return new Tuple4<>(System.currentTimeMillis(), input.f1, sum / count, input.f3);

    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent();
        }
    }
}
