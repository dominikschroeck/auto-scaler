package org.dima.dominikschroeck.largestate.kafka.Producers;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Producer for Price Search Machine (PSM) Benchmark. Produces to Kafka
 */
public class PSM_Producer extends Generic_Producer implements Runnable {
    private Long pause, ChangeEvery;
    private int EventsPerStep_max, EventsPerStep_low, EventsPerStep_initial;
    private ArrayList<String[]> data_feed;

    private Producer<Long, PSM_ClickEvent> PSM_ClickEvent_Producer;
    private Producer<Long, PSM_ImpressionEvent> PSM_ImpressionEvent_Producer;
    private Producer<Long, PSM_SearchEvent> PSM_SearchEvent_Producer;
    private int id;
    private int produced;


    /**
     * Static Final Topic names for Kafka
     */
    private final static String CLICK_TOPIC = "PSM_CLICKS";
    private final static String IMPRESSION_TOPIC = "PSM_IMPRESSIONS";
    private final static String SEARCH_TOPIC = "PSM_SEARCH";


    @Override
    public int getProducedCount() {
        return this.produced;
    }

    @Override
    public int getID() {
        return this.id;
    }

    ;


    /**
     * Constructor that stores the configuration. Check {@link org.dima.dominikschroeck.largestate.kafka.Producers.NexMarkProducer#NexMarkProducer} for more details
     *
     * @param pause
     * @param changeEvery
     * @param eventsPerStep_max
     * @param eventsPerStep_low
     * @param eventsPerStep_initial
     * @param id
     * @param URL URL to datafeed that stores information on products. This is a feed of items/products from the PSM. We need this to simulate the Events.
     */
    public PSM_Producer(Long pause, Long changeEvery, int eventsPerStep_max, int eventsPerStep_low, int eventsPerStep_initial, int id, String URL) {
        this.produced = 0;
        this.pause = pause;
        ChangeEvery = changeEvery;
        EventsPerStep_max = eventsPerStep_max;
        EventsPerStep_low = eventsPerStep_low;
        EventsPerStep_initial = eventsPerStep_initial;
        this.id = id;

        this.data_feed = getCSVfromNet(URL);

        // Remove headlines from data feed.
        data_feed.remove(0);

    }

    /**
     * Setter for PSM Impressions Producer
     * @param PSM_ImpressionEvent_Producer
     */
    public void setPSM_ImpressionEvent_Producer(Producer<Long, PSM_ImpressionEvent> PSM_ImpressionEvent_Producer) {
        this.PSM_ImpressionEvent_Producer = PSM_ImpressionEvent_Producer;
    }


    /**
     * Setter for Click Event Producer
     * @param PSM_ClickEvent_Producer
     */
    public void setPSM_ClickEvent_Producer(Producer<Long, PSM_ClickEvent> PSM_ClickEvent_Producer) {
        this.PSM_ClickEvent_Producer = PSM_ClickEvent_Producer;
    }

    /**
     * Getter for Producer for Search Events
     * @return
     */
    public Producer<Long, PSM_SearchEvent> getPSM_SearchEvent_Producer() {
        return PSM_SearchEvent_Producer;
    }

    /**
     * Setter for PSM Search Event Producer
     * @param PSM_SearchEvent_Producer
     */
    public void setPSM_SearchEvent_Producer(Producer<Long, PSM_SearchEvent> PSM_SearchEvent_Producer) {
        this.PSM_SearchEvent_Producer = PSM_SearchEvent_Producer;
    }

    /**
     * Run method of Thread. Produces events endlessly (Ctrl+C cancels)
     */
    @Override
    public void run() {
        Long next_change = System.currentTimeMillis() + ChangeEvery;
        int EventsPerStep = EventsPerStep_initial;

        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",Alive");
        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",ChangeInterval," + ChangeEvery / 1000 + ",seconds");
        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",UpperBoundBids," + EventsPerStep_max);
        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",LowerBoundBids," + EventsPerStep_low);
        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",Stepsize," + pause + ",milliseconds");
        System.out.println("[" + id + "]," + System.currentTimeMillis() + ",ChangeThroughput," + 0 + "," + EventsPerStep);

        while (true) {

            if (next_change <= System.currentTimeMillis()) {

                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",Alive");
                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",ChangeInterval," + ChangeEvery / 1000 + ",seconds");
                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",UpperBoundBids," + EventsPerStep_max);
                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",LowerBoundBids," + EventsPerStep_low);
                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",Stepsize," + pause + ",milliseconds");
                System.out.println("[" + id + "]," + System.currentTimeMillis() + ",ChangeThroughput," + 0 + "," + EventsPerStep);
                next_change = System.currentTimeMillis() + ChangeEvery;

            }

            // PRODUCING!!!!
            Random randomGenerator = new Random();
            for (int i = 0; i < EventsPerStep; i++) {

                /**
                 * Click or impression events
                 *
                 */
                int impression_or_click = randomGenerator.nextInt(2);
                int line = randomGenerator.nextInt(data_feed.size() - 1);

                String owner = "";
                String product = "";
                String category = "";

                owner = data_feed.get(line)[2];
                product = data_feed.get(line)[0];
                category = data_feed.get(line)[1];

                Double price = 0.0;
                if (impression_or_click == 1) {
                    price = 0.2 + randomGenerator.nextDouble() * 0.5;
                }

                if (impression_or_click == 1) {
                    PSM_ClickEvent_Producer.send(new ProducerRecord<>(CLICK_TOPIC, Long.valueOf(id).longValue(), new PSM_ClickEvent(System.currentTimeMillis(), category, product, owner, price, 0L)));
                } else {
                    PSM_ImpressionEvent_Producer.send(new ProducerRecord<>(IMPRESSION_TOPIC, Long.valueOf(id).longValue(), new PSM_ImpressionEvent(System.currentTimeMillis(), category, product, 0L)));
                }

                produced++;
                Integer rank = ThreadLocalRandom.current().nextInt(200);

                /**
                 * Search Events
                 */
                line = randomGenerator.nextInt(data_feed.size() - 1);

                PSM_SearchEvent_Producer.send(new ProducerRecord<>(SEARCH_TOPIC, Long.valueOf(id).longValue(), new PSM_SearchEvent(System.currentTimeMillis(), data_feed.get(line)[1], data_feed.get(line)[0], rank, 0L)));
                produced++;
            }

            try {
                Thread.sleep(pause);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<String[]> getCSVfromNet(String plain_url) {
        BufferedReader in = null;
        ArrayList<String[]> adverts = new ArrayList<>();
        try {
            URL products_url = new URL(plain_url);


            in = new BufferedReader(new InputStreamReader(products_url.openStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        CSVReader reader = new CSVReader(in);


        try {
            adverts = (ArrayList) reader.readAll();

            reader.close();
            in.close();

            return adverts;
        } catch (IOException e) {
            e.printStackTrace();
        }
        reader = null;
        in = null;
        return adverts;
    }


}
