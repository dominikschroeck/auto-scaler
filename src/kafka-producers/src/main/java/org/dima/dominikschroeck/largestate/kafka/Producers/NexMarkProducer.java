package org.dima.dominikschroeck.largestate.kafka.Producers;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dima.dominikschroeck.largestate.Events.NexMark.*;
import org.dima.dominikschroeck.largestate.kafka.Jobs.NexMark_Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class simply generates auctions, bids, persons and items for an online bidding system analytics system. It is part of Dominik Schroeck's master thesis for DIMA/DFKI
 */
public class NexMarkProducer extends Generic_Producer implements Runnable {

    private Long ChangeEvery;
    private final String AUCTION_TOPIC = "auctionEvents";
    private final String BID_TOPIC = "bidEvents";
    private final String ITEM_TOPIC = "itemEvents";
    private final String PERSON_TOPIC = "personEvents";
    List<Category> categories;
    private ArrayList<SimpleAuction> auctions;

    private int producer_id;
    private Long pause;
    private Long start;
    private int bids_per_step;

    private Producer<Long, AuctionEvent> auctionEventProducer;
    private Producer<Long, BidEvent> bidEventProducer;
    private Producer<Long, NewPersonEvent> newPersonEventProducer;
    private Producer<Long, NewItemEvent> newItemEventProducer;


    private int bids_per_step_max;
    private int bids_per_step_low;

    private int auction_id;
    private int item_id;
    private int person_id;
    private int bid_id;
    private double slope;

    private List<String[]> person_list;
    private ArrayList<String[]> item_names;
    private int produced;


    /**
     * Constructor. Initializes categories and pulls CSV files for cities and item names.
     *
     * @param pause             Pause between two subsequent Production Intervals
     * @param bids_per_step_low Minimum number of bids per interval, all other events are produced at a fraction of this, consult the thesis for more details
     * @param bids_per_step_max Maximum number of bids per interval
     * @param ChangeInterval    Interval in which to (randomly) change the bids per step during execution
     * @param producer_id       ID of this producer thread
     */
    public NexMarkProducer(Long pause, int bids_per_step_low, int bids_per_step_max, Long ChangeInterval, int producer_id) {

        produced = 0;
        slope = 0.05;

        String persons_url = "https://stefanie.dominikschroeck.de/names_and_cities.csv";

        item_names = getCSVfromNet("https://stefanie.dominikschroeck.de/items.csv");
        this.producer_id = producer_id;

        this.ChangeEvery = ChangeInterval;

        this.auctions = new ArrayList<>();

        this.categories = new ArrayList<>();

        this.person_list = getCSVfromNet(persons_url);


        this.pause = pause;
        this.start = System.currentTimeMillis();

        // Initialize Categories
        Integer cat_id = 0;

        this.categories.add(new Category("Auto und Motorrad", "B", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Baby und Spielwaren", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Computer und Software", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Fotografie", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Freizeit und Musik", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Gesundheit und Kostmetik", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Handy und Telefon", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Haushalt", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Heimwerken und Garten", "A", cat_id));
        cat_id += 1;
        this.categories.add(new Category("Lebensmittel und Getraenke", "A", cat_id));
        cat_id += 1;


        //

        auction_id = new Integer(0);
        person_id = new Integer(0);
        item_id = new Integer(0);
        bid_id = new Integer(0);

        this.bids_per_step_max = bids_per_step_max;
        this.bids_per_step_low = bids_per_step_low;
        this.bids_per_step = bids_per_step_low;
        //ThreadLocalRandom.current().nextInt(bids_per_step_low, bids_per_step_max + 1);


    }

    /**
     * Getter for number of produced events
     *
     * @return
     */
    @Override
    public int getProducedCount() {
        return produced;
    }

    /**
     * Getter for ID of this thread
     *
     * @return
     */
    public int getID() {
        return producer_id;
    }

    /**
     * Getter for Producer
     *
     * @return
     */
    public Producer<Long, AuctionEvent> getAuctionEventProducer() {
        return auctionEventProducer;
    }

    /**
     * Setter for Producer
     *
     * @param auctionEventProducer
     */
    public void setAuctionEventProducer(Producer<Long, AuctionEvent> auctionEventProducer) {
        this.auctionEventProducer = auctionEventProducer;
    }

    public Producer<Long, BidEvent> getBidEventProducer() {
        return bidEventProducer;
    }

    public void setBidEventProducer(Producer<Long, BidEvent> bidEventProducer) {
        this.bidEventProducer = bidEventProducer;
    }

    public Producer<Long, NewPersonEvent> getNewPersonEventProducer() {
        return newPersonEventProducer;
    }

    public void setNewPersonEventProducer(Producer<Long, NewPersonEvent> newPersonEventProducer) {
        this.newPersonEventProducer = newPersonEventProducer;
    }

    public Producer<Long, NewItemEvent> getNewItemEventProducer() {
        return newItemEventProducer;
    }

    public void setNewItemEventProducer(Producer<Long, NewItemEvent> newItemEventProducer) {
        this.newItemEventProducer = newItemEventProducer;
    }

    /**
     * Run method of Thread. Generates the events as specified by the benchmark and configuration as specified from constructor (Config file)
     */
    @Override
    public void run() {

        boolean hundred_k = false;

        // Generiere jeweils die Events
        Long next_change = System.currentTimeMillis() + ChangeEvery;

        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",Alive");
        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",ChangeInterval," + ChangeEvery / 1000 + ",seconds");
        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",UpperBoundBids," + bids_per_step_max);
        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",LowerBoundBids," + bids_per_step_low);
        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",Stepsize," + pause + ",milliseconds");
        System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",ChangeThroughput," + 0 + "," + bids_per_step);


        while (true) {
            if (auctions.size() >= 100000) hundred_k = true;
            else hundred_k = false;
            if (next_change <= System.currentTimeMillis()) {
                int current = bids_per_step;

                // INcrease input rate ever further until we reach the final
                this.bids_per_step = (int) (this.bids_per_step * (1 + slope));

                if (this.bids_per_step >= bids_per_step_max) this.bids_per_step = bids_per_step_max;

                // ThreadLocalRandom.current().nextInt(bids_per_step_low, bids_per_step_max + 1);
                System.out.println("[" + producer_id + "]," + System.currentTimeMillis() + ",ChangeThroughput," + current + "," + bids_per_step);
                next_change = System.currentTimeMillis() + ChangeEvery;

            }

            /**
             * NewPerson Events
             */
            for (int i = 0; i < bids_per_step / 10; i++) {

                // public Person(String name, String email, Long credit_card, String city, String state) {
                // GET names from a random List :) Same for emails and creditcard?

                synchronized (NexMark_Job.person_lock) {

                    NexMark_Job.last_person += 1;
                    int random_name_selector = ThreadLocalRandom.current().nextInt(person_list.size());
                    String name = person_list.get(random_name_selector)[0];
                    String city = person_list.get(random_name_selector)[1];
                    String state = person_list.get(random_name_selector)[2];
                    //(Long timestamp, Integer person_id, String name, String email, Integer creditcard, String city, String state, Long ingestion_timestamp)
                    NewPersonEvent person = new NewPersonEvent(System.currentTimeMillis(), NexMark_Job.last_person, name, name + "@gmail.com", 426350451, city, state, 0L);


                    newPersonEventProducer.send(new ProducerRecord<Long, NewPersonEvent>(PERSON_TOPIC, Long.valueOf(producer_id).longValue(), person));
                    produced++;
                }

            }


            /**
             * New Item Events: #bids / 100
             */
            for (int i = 0; i < bids_per_step / 10; i++) {
                Category randomCategory = categories.get(ThreadLocalRandom.current().nextInt(categories.size()));

                synchronized (NexMark_Job.last_item) {
                    NexMark_Job.last_item += 1;

                    String item_name = item_names.get(ThreadLocalRandom.current().nextInt(item_names.size() - 1))[0];


                    NewItemEvent item = new NewItemEvent(System.currentTimeMillis(), randomCategory.getId(), item_name, item_name + "dgjsghsfjgh", NexMark_Job.last_item, 0L);
                    //System.out.println("[" + producer_id + "] producing ITEM: " + NexMark_Job.last_item);


                    newItemEventProducer.send(new ProducerRecord<Long, NewItemEvent>(ITEM_TOPIC, Long.valueOf(producer_id).longValue(), item));
                    produced++;

                }
            }


            /**
             * openAuction Events
             */
            if (hundred_k == false) { // If we have more than 200k auctions, do not produce more please
                for (int i = 0; i < bids_per_step / 10 ; i++) {
                    int randomItem = ThreadLocalRandom.current().nextInt(NexMark_Job.last_item);
                    int personRandom = ThreadLocalRandom.current().nextInt(NexMark_Job.last_person);
                    Long duration = 10000L + ThreadLocalRandom.current().nextLong(10000L);
                    Double random_price = 2.0 + ThreadLocalRandom.current().nextDouble(3.0);
                    Double reserve = random_price * 1.5;
                    synchronized (NexMark_Job.last_auction) {
                        NexMark_Job.last_auction += 1;
                        AuctionEvent auction = new AuctionEvent(System.currentTimeMillis(), NexMark_Job.last_auction, personRandom, randomItem,
                                random_price, reserve, System.currentTimeMillis() + 1000L, System.currentTimeMillis() + 1000L + duration, 0L);

                        auctions.add(new SimpleAuction(auction.getAuction_id(), auction.getIntialPrice(), auction.getEnd()));
                        auctionEventProducer.send(new ProducerRecord<>(AUCTION_TOPIC, Long.valueOf(producer_id).longValue(), auction));
                    }
                    produced++;

                }
            }


            /**
             * Bid Events
             */
            for (int i = 0; i < bids_per_step; i++) {
                if (auctions.size() > 0){
                produced++;
                int random_person = ThreadLocalRandom.current().nextInt(NexMark_Job.last_person);
                int random_auction = ThreadLocalRandom.current().nextInt(auctions.size());

                double price = auctions.get(random_auction).getIntialPrice() * (1 + ThreadLocalRandom.current().nextDouble() + 0.0001);

                /**
                 * Updating the price after the bid
                 */
                auctions.get(random_auction).setIntialPrice(price);


                // Long timestamp, Integer auction_id, Integer person_id, Integer bid_id, Double bid, Long ingestion_timestamp) {
                BidEvent bid = new BidEvent(System.currentTimeMillis(), auctions.get(random_auction).getAuction_id(), random_person, bid_id, price, 0L);

                bid_id += 1;
                bidEventProducer.send(new ProducerRecord<>(BID_TOPIC, Long.valueOf(producer_id).longValue(), bid));
                produced++;


            }}


            /**
             * Clean up expired auctions
             */
            Long stamp = System.currentTimeMillis();
            for (ListIterator<SimpleAuction> i = auctions.listIterator(); i.hasNext(); ) {
                SimpleAuction auction = i.next();

                if (auction.getEnd() <= stamp) {
                    i.remove();
                }


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