package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class ClickEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<PSM_ClickEvent> {
    @Override
    public void write(Kryo kryo, Output output, PSM_ClickEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeLong ( psm_event.timestamp );
        output.writeDouble ( psm_event.price );
        output.writeString ( psm_event.owner );


    }

    @Override
    public PSM_ClickEvent read(Kryo kryo, Input input, Class<PSM_ClickEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Long timestamp = input.readLong ( );
        Double price = input.readDouble ( );
        String owner = input.readString ( );


        return new PSM_ClickEvent ( timestamp, category, product, owner, price, System.currentTimeMillis ( ) );
    }
}