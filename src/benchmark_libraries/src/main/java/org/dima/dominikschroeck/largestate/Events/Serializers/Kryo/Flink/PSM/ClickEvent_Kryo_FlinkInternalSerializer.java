package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class ClickEvent_Kryo_FlinkInternalSerializer
        extends com.esotericsoftware.kryo.Serializer<PSM_ClickEvent> {


    @Override
    public void write(Kryo kryo, Output output, PSM_ClickEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeLong ( psm_event.timestamp );
        output.writeDouble ( psm_event.price );
        output.writeString ( psm_event.owner );
        output.writeLong ( psm_event.ingestion_stamp );


    }

    @Override
    public PSM_ClickEvent read(Kryo kryo, Input input, Class<PSM_ClickEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Long timestamp = input.readLong ( );
        Double price = input.readDouble ( );
        String owner = input.readString ( );
        Long ingestion_stamp = input.readLong ( );


        return new PSM_ClickEvent ( timestamp, category, product, owner, price, ingestion_stamp );
    }
}