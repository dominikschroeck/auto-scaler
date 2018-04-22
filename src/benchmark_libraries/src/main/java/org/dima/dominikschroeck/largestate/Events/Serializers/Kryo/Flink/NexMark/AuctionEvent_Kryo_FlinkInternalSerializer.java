package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class AuctionEvent_Kryo_FlinkInternalSerializer
        extends com.esotericsoftware.kryo.Serializer<AuctionEvent> {
    @Override
    public void write(Kryo kryo, Output output, AuctionEvent event) {
        // (Long timestamp, Integer auction_id, Integer person_id, Integer item_id, Double intialPrice, Double reserve, Long start, Long end, Long ingestion_timestamp
        output.writeLong ( event.timestamp );
        output.writeInt ( event.auction_id );
        output.writeInt ( event.item_id );
        output.writeInt ( event.person_id );
        output.writeDouble ( event.intialPrice );
        output.writeDouble ( event.reserve );
        output.writeLong ( event.start );
        output.writeLong ( event.end );
        output.writeLong ( event.ingestion_timestamp );
    }

    @Override
    public AuctionEvent read(Kryo kryo, Input input, Class<AuctionEvent> aClass) {
        // (Long timestamp, Integer auction_id, Integer person_id, Integer item_id, Double intialPrice, Double reserve, Long start, Long end, Long ingestion_timestamp
        Long timestamp = input.readLong ( );
        Integer auction_id = input.readInt ( );
        Integer item_id = input.readInt ( );
        Integer person_id = input.readInt ( );
        Double initialPrice = input.readDouble ( );
        Double reserve = input.readDouble ( );
        Long start = input.readLong ( );
        Long end = input.readLong ( );
        Long ingestion_timestamp = input.readLong ( );


        return new AuctionEvent ( timestamp, auction_id, person_id, item_id, initialPrice, reserve, start, end, ingestion_timestamp );
    }
}
