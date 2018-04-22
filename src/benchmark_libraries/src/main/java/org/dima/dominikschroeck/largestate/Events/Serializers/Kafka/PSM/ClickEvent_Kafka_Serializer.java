package org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM.ClickEvent_Kryo_Serializer;

import java.util.Map;


public class ClickEvent_Kafka_Serializer implements Serializer<PSM_ClickEvent>, Deserializer<PSM_ClickEvent> {


    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( PSM_ClickEvent.class, new ClickEvent_Kryo_Serializer ( ) );
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
    public PSM_ClickEvent deserialize(String topic, byte[] message) {
        PSM_ClickEvent out = kryos.get ( ).readObject ( new ByteBufferInput ( message ), PSM_ClickEvent.class );
        out.setIngestion_stamp ( System.currentTimeMillis ( ) );
        return out;
    }


    @Override
    public byte[] serialize(String topic, PSM_ClickEvent element) {
        int byteBufferLength = 64 / 8 + element.getCategory ( ).length ( ) + element.getProduct ( ).length ( ) + element.getOwner ( ).length ( ) + 64 / 8 + 10;

        ByteBufferOutput output = new ByteBufferOutput ( byteBufferLength );
        kryos.get ( ).writeObject ( output, element );

        return output.toBytes ( );
    }


}
