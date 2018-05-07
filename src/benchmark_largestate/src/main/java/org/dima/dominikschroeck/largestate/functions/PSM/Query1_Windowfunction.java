package org.dima.dominikschroeck.largestate.functions.PSM;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Windowfunction for query 1 of the PSM Job
 */
public class Query1_Windowfunction extends RichWindowFunction<PSM_ClickEvent, Tuple6<Long, Long, String, String, Long, Long>, Tuple, TimeWindow> {

    private HashMap<String, Long> category_to_count;
    private Long latency = 0L;
    private Long waitingTime = 0L;
    private Meter meter;
    private Long overall_latency = 0L;
    private Long parallelism = 0L;

    public Query1_Windowfunction(Integer parallelism) {
        super();
        this.category_to_count = new HashMap<>();
        this.parallelism = parallelism.longValue();

    }

    @Override
    public void open(Configuration parameters) throws Exception {


        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query1.OverallLatency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return overall_latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query1_WindowFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query1_WindowFunction.WaitingTime", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query1_WindowFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });


        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query1_WindowFunction.Throughput", new DropwizardMeterWrapper(meter));


        super.open(parameters);



    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<PSM_ClickEvent> iterable, Collector<Tuple6<Long, Long, String, String, Long, Long>> collector) {
        // Now count per category
        Long start = System.currentTimeMillis();
        Long latest_timestamp = 0L;
        Long input_stamp = 0L;
        Long count_elems=0L;

        Long highestCategory_Count = 0L;
        for (Iterator<PSM_ClickEvent> iter = iterable.iterator(); iter.hasNext(); ) {

            PSM_ClickEvent elem = iter.next();
            if (elem.getIngestion_stamp() > latest_timestamp) {
                latest_timestamp = elem.getIngestion_stamp();
            }
            count_elems = count_elems +1;

            if (elem.ingestion_stamp > input_stamp) input_stamp = elem.getIngestion_stamp();
            //this.waitingTime = System.currentTimeMillis ( ) - (timeWindow.getStart ( ) - timeWindow.getEnd ( )) - elem.getIngestion_stamp ( );

            String category = elem.getCategory();
            if (category_to_count.containsKey(category)) {
                Long count = category_to_count.get(category);
                count += 1;
                if (count > highestCategory_Count) {
                    highestCategory_Count = count;

                }
                category_to_count.remove(category);
                category_to_count.put(category, count);

            } else {
                category_to_count.put(category, 1L);

            }
            this.meter.markEvent();
        }




        String owner = iterable.iterator().next().getOwner();
        for (Iterator<Map.Entry<String, Long>> iter = category_to_count.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, Long> elem = iter.next();
            if (elem.getValue() >= highestCategory_Count) {
                collector.collect(new Tuple6<>(System.currentTimeMillis(), timeWindow.getEnd(), elem.getKey(), owner, elem.getValue(), latest_timestamp));
            }

        }
        this.waitingTime = start - input_stamp;
        this.latency = System.currentTimeMillis() - start;
        this.overall_latency = System.currentTimeMillis() - latest_timestamp;
    }


}
