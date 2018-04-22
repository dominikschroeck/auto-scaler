package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;

import java.util.Iterator;

public class Query5_RichCoGroupFunction extends RichCoGroupFunction<PSM_ClickEvent, PSM_SearchEvent, Tuple4<Long, String, Double, Long>> {

    private Long latency = 0L;
    private Meter meter;
    private Long waitingtime = 0L;
    private Long overall_latency = 0L;

    public Query5_RichCoGroupFunction() {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_RichCoGroupFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query5_RichCoGroupFunction.Throughput", new DropwizardMeterWrapper(meter));


        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query5_RichCoGroupFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingtime;
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
        super.open(parameters);
    }

    @Override
    public void coGroup(Iterable<PSM_ClickEvent> clicks, Iterable<PSM_SearchEvent> searches, Collector<Tuple4<Long, String, Double, Long>> collector) {
        // Now here we count searches and clicks and finally return one ratio: searches / clicks
        // COUNT OF CLICKS:
        Long start = System.currentTimeMillis();
        Long latest_timestamp = 0L;

        Long input_stamp = 0L;

        Long click_count = 0L;
        String product = "";
        Iterator<PSM_ClickEvent> iter_prod = clicks.iterator();
        if (iter_prod.hasNext()){
            product = clicks.iterator().next().getProduct();
        }

        //String product = clicks.iterator().next().getProduct();
        int count = 0;
        Iterator<PSM_ClickEvent> iterator = clicks.iterator();
        while (iterator.hasNext()) {
            PSM_ClickEvent elem = iterator.next();
            count++;

            if (elem.getIngestion_stamp() > latest_timestamp) {
                latest_timestamp = elem.getIngestion_stamp();
            }

            meter.markEvent();

        }
        click_count = Long.valueOf(count);


        // COUNT OF SEARCHES
        Long search_count = 0L;

        count = 0;
        Iterator<PSM_SearchEvent> search_iterator = searches.iterator();
        while (search_iterator.hasNext()) {
            PSM_SearchEvent elem = search_iterator.next();
            count++;

            if (elem.getIngestion_stamp() > latest_timestamp) {
                latest_timestamp = elem.getIngestion_stamp();
            }
            meter.markEvent();

        }
        search_count = Long.valueOf(count);


        if (product !=""){

        collector.collect(new Tuple4<>(System.currentTimeMillis(), product, Double.valueOf(search_count) / Double.valueOf(click_count), latest_timestamp));}
        this.waitingtime = start - latest_timestamp;
        this.overall_latency = System.currentTimeMillis() - latest_timestamp;
        this.latency = System.currentTimeMillis() - start;
    }

}
