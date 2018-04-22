package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.NexMark.BidEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class BidEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<BidEvent> {
    @Override
    public void write(Kryo kryo, Output output, BidEvent event) {
        // Long timestamp, Integer auction_id, Integer person_id, Integer bid_id, Double bid, Long ingestion_timestamp
        output.writeLong ( event.timestamp );
        output.writeInt ( event.auction_id );
        output.writeInt ( event.person_id );
        output.writeInt ( event.bid_id );
        output.writeDouble ( event.bid );


    }

    @Override
    public BidEvent read(Kryo kryo, Input input, Class<BidEvent> aClass) {
        // Long timestamp, Integer auction_id, Integer person_id, Integer bid_id, Double bid, Long ingestion_timestamp
        Long timestamp = input.readLong ( );
        Integer auction_id = input.readInt ( );
        Integer person_id = input.readInt ( );
        Integer bid_id = input.readInt ( );
        Double bid = input.readDouble ( );

        Long ingestion_timestamp = System.currentTimeMillis ( );


        return new BidEvent ( timestamp, auction_id, person_id, bid_id, bid, ingestion_timestamp );
    }
}
