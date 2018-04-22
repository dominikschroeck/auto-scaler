package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class ImpressionEvent_Kryo_FlinkInternalSerializer
        extends com.esotericsoftware.kryo.Serializer<PSM_ImpressionEvent> {
    @Override
    public void write(Kryo kryo, Output output, PSM_ImpressionEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeLong ( psm_event.timestamp );
        output.writeLong ( psm_event.ingestion_stamp );

    }

    @Override
    public PSM_ImpressionEvent read(Kryo kryo, Input input, Class<PSM_ImpressionEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Long timestamp = input.readLong ( );
        Long ingestion_stamp = input.readLong ( );

        return new PSM_ImpressionEvent ( timestamp, category, product, ingestion_stamp );
    }
}