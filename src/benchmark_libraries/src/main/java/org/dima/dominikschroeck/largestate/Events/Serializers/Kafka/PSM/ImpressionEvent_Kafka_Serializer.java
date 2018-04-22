package org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ImpressionEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM.ImpressionEvent_Kryo_Serializer;

import java.util.Map;


public class ImpressionEvent_Kafka_Serializer implements Serializer<PSM_ImpressionEvent>, Deserializer<PSM_ImpressionEvent> {


    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( PSM_ImpressionEvent.class, new ImpressionEvent_Kryo_Serializer ( ) );
            return kryo;
        }
    };

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public PSM_ImpressionEvent deserialize(String topic, byte[] message) {
        PSM_ImpressionEvent out = kryos.get ( ).readObject ( new ByteBufferInput ( message ), PSM_ImpressionEvent.class );
        out.setIngestion_stamp ( System.currentTimeMillis ( ) );
        return out;
    }


    @Override
    public byte[] serialize(String topic, PSM_ImpressionEvent element) {
        int byteBufferLength = 8 + element.getCategory ( ).length ( ) + element.getProduct ( ).length ( ) + 100;
        ByteBufferOutput output = new ByteBufferOutput ( byteBufferLength );
        kryos.get ( ).writeObject ( output, element );

        return output.toBytes ( );
    }


}
