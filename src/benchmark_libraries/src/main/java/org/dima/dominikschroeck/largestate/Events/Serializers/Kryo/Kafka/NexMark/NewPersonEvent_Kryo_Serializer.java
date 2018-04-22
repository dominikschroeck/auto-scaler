package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewPersonEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class NewPersonEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<NewPersonEvent> {
    @Override
    public void write(Kryo kryo, Output output, NewPersonEvent event) {
        // (Long timestamp, Integer person_id, String email, Integer creditcard, String city, String state, Long ingestion_timestamp)
        output.writeLong ( event.timestamp );
        output.writeInt ( event.person_id );
        output.writeInt ( event.creditcard );
        output.writeString ( event.email );
        output.writeString ( event.name );
        output.writeString ( event.city );
        output.writeString ( event.state );


    }

    @Override
    public NewPersonEvent read(Kryo kryo, Input input, Class<NewPersonEvent> aClass) {
        // (Long timestamp, Integer person_id, String email, Integer creditcard, String city, String state, Long ingestion_timestamp)
        Long timestamp = input.readLong ( );
        Integer person_id = input.readInt ( );
        Integer creditcard = input.readInt ( );
        String email = input.readString ( );
        String name = input.readString ( );
        String city = input.readString ( );
        String state = input.readString ( );

        Long ingestion_timestamp = System.currentTimeMillis ( );

        return new NewPersonEvent ( timestamp, person_id, name, email, creditcard, city, state, ingestion_timestamp );
    }
}
