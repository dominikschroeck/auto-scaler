package org.dima.dominikschroeck.largestate.kafka.Monitoring;

import org.dima.dominikschroeck.largestate.kafka.Producers.Generic_Producer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simple Monitoring Tool for Producer KPIs. Producers that implement {@link Generic_Producer} can register. This object will regularly monitor the number of events produced and possibly more KPIs, but so far not implemented. Output is currently limited to stdout.
 */
public class Production_Monitoring implements  Runnable{
    private List<Generic_Producer> producer_list;
    private int watch_interval = 10000;

    /**
     * Instantiates a list of producers.
     */
    public Production_Monitoring(){
        this.producer_list = new ArrayList<>();
    }

    /**
     * Producers register here in order to be accessible for the Monitor.
     * @param producer
     */
    public void registerProducer(Generic_Producer producer){
        System.out.println("[MONITORING-INFO] New Producer Thread registered. ID: " + producer.getID());

        synchronized (producer_list){
            producer_list.add(producer);
        }
    }

    /**
     * Run method of the Thread. Regularly emits KPIs to stdout.
     */
    @Override
    public void run() {
        while (true){
            synchronized (producer_list){
                for (Iterator<Generic_Producer> iter = producer_list.iterator(); iter.hasNext();){
                    Generic_Producer producer = iter.next();
                    System.out.println(System.currentTimeMillis() +",[MONITORING-INFO] Producer: " + producer.getID() + "," + producer.getProducedCount() + " Events produced");
                }
            }
            try {
                Thread.sleep(watch_interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
