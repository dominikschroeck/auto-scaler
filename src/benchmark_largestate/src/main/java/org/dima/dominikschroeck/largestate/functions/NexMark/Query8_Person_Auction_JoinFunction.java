package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewPersonEvent;

/**
 * Join Function that fires the person's details every time there was a new auction by a new person over a timewindow
 */
public class Query8_Person_Auction_JoinFunction extends RichJoinFunction<NewPersonEvent, AuctionEvent, Tuple4<Long, Integer, String, Long>> {
    private Meter meter;
    private Long latency = 0L;
    private Long waitingTime_1 = 0L;
    private Long waitingTime_2 = 0L;
    private Long parallelism;

    public Query8_Person_Auction_JoinFunction(Integer parallelism) {
        this.parallelism = parallelism.longValue();

        this.latency = 0L;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query8_Person_Auction_JoinFunction.Latency", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return latency;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query8_Person_Auction_JoinFunction.WaitingTime_1", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_1;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query8_Person_Auction_JoinFunction.Parallelism", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return parallelism;
                    }
                });

        getRuntimeContext()
                .getMetricGroup()
                .gauge("Query8_Person_Auction_JoinFunction.WaitingTime_2", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return waitingTime_2;
                    }
                });

        // Throughput
        com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("Query8_Person_Auction_JoinFunction.Throughput", new DropwizardMeterWrapper(meter));
        super.open(parameters);
    }

    /**
     * Join Auction and Person on name and return the Persons name as well as ID. Finding every person that opened an auction in the last x minutes
     *
     * @param person
     * @param auction
     * @return Tuple5
     * @throws Exception
     */
    @Override
    public Tuple4<Long, Integer, String, Long> join(NewPersonEvent person, AuctionEvent auction) {
        Long start = System.currentTimeMillis();
        Long latency_stamp = 0L;
        if (auction.getIngestion_timestamp() > person.getIngestion_timestamp()) {
            latency_stamp = auction.getIngestion_timestamp();
        } else {
            latency_stamp = person.getIngestion_timestamp();
        }
        this.markEvent();
        this.waitingTime_1 = start - person.getIngestion_timestamp();
        this.waitingTime_2 = start - auction.getIngestion_timestamp();
        Integer person_id = person.getPerson_id();
        String person_name = person.getName();
        Long timestamp = System.currentTimeMillis();

        this.latency = System.currentTimeMillis() - start;

        return new Tuple4<>(timestamp, person_id, person_name, latency_stamp);
    }

    private void markEvent() {
        if (meter != null) {
            meter.markEvent(2);
        }
    }
}

