package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewItemEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class NewItemEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<NewItemEvent> {
    @Override
    public void write(Kryo kryo, Output output, NewItemEvent event) {
        // (Long timestamp, Integer category_id, String name, String description, Integer item_id, Long ingestion_timestamp)
        output.writeLong ( event.timestamp );
        output.writeInt ( event.category_id );
        output.writeString ( event.name );
        output.writeString ( event.description );
        output.writeInt ( event.item_id );


    }

    @Override
    public NewItemEvent read(Kryo kryo, Input input, Class<NewItemEvent> aClass) {

        Long timestamp = input.readLong ( );
        Integer category_id = input.readInt ( );
        String name = input.readString ( );
        String description = input.readString ( );
        Integer item_id = input.readInt ( );

        Long ingestion_timestamp = System.currentTimeMillis ( );


        return new NewItemEvent ( timestamp, category_id, name, description, item_id, ingestion_timestamp );
    }
}
