package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;

/**
 * These Kryo Serializers are used for serializing events produced in the Kafka Producers.
 */
public class SearchEvent_Kryo_Serializer
        extends com.esotericsoftware.kryo.Serializer<PSM_SearchEvent> {
    @Override
    public void write(Kryo kryo, Output output, PSM_SearchEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeInt ( psm_event.rank );
        output.writeLong ( psm_event.timestamp );

    }

    @Override
    public PSM_SearchEvent read(Kryo kryo, Input input, Class<PSM_SearchEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Integer rank = input.readInt ( );
        Long timestamp = input.readLong ( );

        return new PSM_SearchEvent ( timestamp, category, product, rank, System.currentTimeMillis ( ) );
    }
}