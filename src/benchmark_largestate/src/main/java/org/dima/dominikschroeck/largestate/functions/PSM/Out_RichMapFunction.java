package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

public class Out_RichMapFunction extends RichMapFunction<Tuple, String> {

    public Long latency = 0L;
    private String name;

    public Out_RichMapFunction() {
        super();
    }

    public Out_RichMapFunction(String name) {
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(this.name + ".OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });
    }


    @Override
    public String map(Tuple tuple) throws Exception {
        this.latency = System.currentTimeMillis() - (Long) tuple.getField(tuple.getArity() - 1);
        return tuple.toString();
    }

}
