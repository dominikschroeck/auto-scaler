package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent_Category;
import scala.Int;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class AuctionEvent_Category_Kryo_FlinkInternalSerializer
        extends com.esotericsoftware.kryo.Serializer<AuctionEvent_Category> {
    @Override
    public void write(Kryo kryo, Output output, AuctionEvent_Category event) {
        output.writeLong ( event.timestamp );
        output.writeInt ( event.auction_id );
        output.writeDouble ( event.intialPrice );
        output.writeLong ( event.end );
        output.writeInt ( event.category_id );
        output.writeLong ( event.ingestion_timestamp );

    }

    @Override
    public AuctionEvent_Category read(Kryo kryo, Input input, Class<AuctionEvent_Category> aClass) {
        Long timestamp = input.readLong ( );
        Integer auction_id = input.readInt ( );
        Double initialPrice = input.readDouble ( );
        Long end = input.readLong ( );
        Integer category = input.readInt ( );
        Long ingestion_timestamp = input.readLong ( );
        return new AuctionEvent_Category ( timestamp, auction_id, initialPrice, end, category, ingestion_timestamp);
    }
}
