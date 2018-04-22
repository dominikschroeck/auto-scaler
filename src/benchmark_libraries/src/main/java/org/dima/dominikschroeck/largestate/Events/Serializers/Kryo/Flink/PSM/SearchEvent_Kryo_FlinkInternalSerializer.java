package org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Flink.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class SearchEvent_Kryo_FlinkInternalSerializer
        extends com.esotericsoftware.kryo.Serializer<PSM_SearchEvent> {

    @Override
    public void write(Kryo kryo, Output output, PSM_SearchEvent psm_event) {
        output.writeString ( psm_event.category );
        output.writeString ( psm_event.product );
        output.writeInt ( psm_event.rank );
        output.writeLong ( psm_event.timestamp );
        output.writeLong ( psm_event.ingestion_stamp );
    }

    @Override
    public PSM_SearchEvent read(Kryo kryo, Input input, Class<PSM_SearchEvent> aClass) {
        String category = input.readString ( );
        String product = input.readString ( );
        Integer rank = input.readInt ( );
        Long timestamp = input.readLong ( );
        Long ingestion_stamp = input.readLong ( );

        return new PSM_SearchEvent ( timestamp, category, product, rank, ingestion_stamp );
    }






}