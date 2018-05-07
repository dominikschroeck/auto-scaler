package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;

import java.util.Iterator;

public class Query4_RichCoGroupFunction extends RichCoGroupFunction<PSM_ClickEvent,
        PSM_ImpressionEvent,
        Tuple4<Long, String, Double, Long>> {

    private Long latency = 0L;
    private Long waitingTime = 0L;
    private Meter meter;
    private Long overall_latency = 0L;
    private Long parallelism = 0L;


    public Query4_RichCoGroupFunction(Integer parallelism){this.parallelism = parallelism.longValue();}


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
                .gauge("Query4_RichCoGroupFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_RichCoGroupFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query4_RichCoGroupFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query4_RichCoGroupFunction.Throughput", new DropwizardMeterWrapper(meter));
        super.open(parameters);
    }

    @Override
    public void coGroup(Iterable<PSM_ClickEvent> clicks,
                        Iterable<PSM_ImpressionEvent> impressions,
                        Collector<Tuple4<Long, String, Double, Long>> collector) {

        Long start = System.currentTimeMillis();
        // SIMPLY COUNT CLICKS AND THEN COUNT IMPRESSIONS

        String key =" ";

        Iterator<PSM_ClickEvent> iter_key = clicks.iterator();

        if (iter_key.hasNext()){

        key = clicks.iterator().next().getProduct();}
        Long impression_count = 0L;
        Long click_count = 0L;
        Long latest_stamp = 0L;
        Long input_stamp = 0L;

        for (Iterator<PSM_ClickEvent> iter = clicks.iterator(); iter.hasNext(); ) {
            PSM_ClickEvent elem = iter.next();
            click_count++;
            if (elem.getIngestion_stamp() > latest_stamp) {
                latest_stamp = elem.getIngestion_stamp();
            }
            this.meter.markEvent();
            if (elem.ingestion_stamp > input_stamp) input_stamp = elem.getIngestion_stamp();


        }
        for (Iterator<PSM_ImpressionEvent> iter = impressions.iterator(); iter.hasNext(); ) {
            PSM_ImpressionEvent elem = iter.next();
            impression_count++;
            if (elem.getIngestion_stamp() > latest_stamp) {
                latest_stamp = elem.getIngestion_stamp();
            }

            if (elem.ingestion_stamp > input_stamp) input_stamp = elem.getIngestion_stamp();
            this.meter.markEvent();

        }

        // OUT: Now, Key, Impression to Clicks, Latest Ingestion Time timestamp

        if(key != ""){
            collector.collect(new Tuple4<>(System.currentTimeMillis(), key, Double.valueOf(impression_count) / Double.valueOf(click_count), latest_stamp));}
        this.waitingTime = start - input_stamp;
        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - latest_stamp;

    }
}
