package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Utils.Quantile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class computes the xth quantile over a window of Events
 */
public class Query2_RichWindowFunction extends RichWindowFunction
        <PSM_ClickEvent,
                Tuple4<Long, String, Double, Long>, Tuple, GlobalWindow> {


    private Integer quantile;
    private Long latency = 0L;
    private Long waitingTime = 0L;
    private Meter meter;
    private Long overall_latency = 0L;

    /**
     * Set the quantile in the constructor that you want to be computed
     *
     * @param quantile
     */
    public Query2_RichWindowFunction(Integer quantile) {
        this.quantile = quantile;
    }

    /**
     * Default constructor. Sets quantile to 50 (Median)
     */
    public Query2_RichWindowFunction() {
        super();
        this.quantile = 50;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query2.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query2_RichWindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query2_RichWindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query2_RichWindowFunction.Throughput", new DropwizardMeterWrapper(meter));
        super.open(parameters);
       // super.open(parameters);
    }

    @Override
    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<PSM_ClickEvent> clicks, Collector<Tuple4<Long, String, Double, Long>> collector) {
        // I have to store each cost, sort them and get the quantile

        Long start = System.currentTimeMillis();
        Long input_stamp = 0L;
        Long count = 0L;

        // Extract double values into a list and sort
        List<Double> prices = new ArrayList<Double>();
        String key = clicks.iterator().next().getProduct();
        Long latest_timestamp = 0L;

        for (Iterator<PSM_ClickEvent> iter = clicks.iterator(); iter.hasNext(); ) {
            PSM_ClickEvent elem = iter.next();
            count = count +1;
            if (elem.getIngestion_stamp() > latest_timestamp) {
                latest_timestamp = elem.getIngestion_stamp();
            }
            if (elem.ingestion_stamp > input_stamp) input_stamp = elem.getIngestion_stamp();
            //this.waitingTime = System.currentTimeMillis ( ) - elem.getIngestion_stamp ( );
            prices.add(elem.getPrice());
            this.meter.markEvent();
        }

        Double quantile = Quantile.quantile(prices, this.quantile);

        // Timestamp, Product, AVG(COST), Latest Ingestion Time timestamp
        collector.collect(new Tuple4<>(System.currentTimeMillis(), key, quantile, latest_timestamp));
        this.waitingTime = start - input_stamp;
        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - latest_timestamp;
    }


}
