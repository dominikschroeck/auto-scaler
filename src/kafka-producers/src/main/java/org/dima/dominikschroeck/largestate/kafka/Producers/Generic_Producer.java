package org.dima.dominikschroeck.largestate.kafka.Producers;

/**
 * Abstract class for Producer Threads
 */
public abstract class Generic_Producer  implements Runnable {

    public int getProducedCount(){ return 0;}
    public int getID(){ return 0;};


}
