package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class ImpressionEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<PSM_ImpressionEvent> {
    @Override
    public void write(Kryo kryo, Output output, PSM_ImpressionEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeLong ( psm_event.timestamp );

    }

    @Override
    public PSM_ImpressionEvent read(Kryo kryo, Input input, Class<PSM_ImpressionEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Long timestamp = input.readLong ( );

        return new PSM_ImpressionEvent ( timestamp, category, product, System.currentTimeMillis ( ) );
    }
}