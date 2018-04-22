package org.dima.dominikschroeck.largestate.functions.Email;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;

public class Query1_CTR_FlatMap extends RichMapFunction<Tuple4<Long, String, String, String>, Tuple3<Long, String, Double>> {
    private transient ValueState<Tuple2<Long, Long>> state;// Left: Clicks, Right: Open CTR: Clicks gegen Open
    private Meter meter;
    private Long waitingTime, latency;

    @Override
    public void open(Configuration parameters) throws Exception {
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Email_Query1_CTR_Map.Throughput", new DropwizardMeterWrapper(meter));

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Email_Query1_CTR_Map.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Email_Query1_CTR_Map.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "CTR", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set

        state = getRuntimeContext().getState(descriptor);

        super.open(parameters);
    }


    @Override
    public Tuple3<Long, String, Double> map(Tuple4<Long, String, String, String> event) throws Exception {
        Long start = System.currentTimeMillis();
        Tuple2<Long, Long> current_state = state.value();
        this.waitingTime = System.currentTimeMillis() - event.f0;
        if (meter != null) {
            meter.markEvent();
        }

        if (event.f3.equals("OPEN")) {
            current_state.f1 = current_state.f1 + 1;
        } else if (event.f3.equals("CLICK")) {
            current_state.f0 = current_state.f0 + 1;
        }
        state.update(current_state);
        this.latency = System.currentTimeMillis() - start;
        if (!current_state.f0.equals(0L)) {
            return new Tuple3<>(System.currentTimeMillis(), event.f1, Double.valueOf(current_state.f1 / current_state.f0).doubleValue());
        } else {
            return new Tuple3<>(System.currentTimeMillis(), event.f1, 0D);
        }

    }
}
