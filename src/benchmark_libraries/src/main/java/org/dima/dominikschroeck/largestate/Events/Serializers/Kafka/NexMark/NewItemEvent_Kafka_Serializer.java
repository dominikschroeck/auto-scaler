package org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.dima.dominikschroeck.largestate.Events.NexMark.NewItemEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark.NewItemEvent_Kryo_Serializer;

import java.util.Map;


public class NewItemEvent_Kafka_Serializer implements Serializer<NewItemEvent>, Deserializer<NewItemEvent> {


    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( NewItemEvent.class, new NewItemEvent_Kryo_Serializer ( ) );
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
    public NewItemEvent deserialize(String topic, byte[] message) {
        NewItemEvent out = kryos.get ( ).readObject ( new ByteBufferInput ( message ), NewItemEvent.class );
        out.setIngestion_timestamp ( System.currentTimeMillis ( ) );
        return out;
    }


    @Override
    public byte[] serialize(String topic, NewItemEvent element) {
        int byteBufferLength = 64 / 8 + 32 / 8 + element.getName ( ).length ( ) + element.getDescription ( ).length ( ) + 32 / 8 + 1;

        ByteBufferOutput output = new ByteBufferOutput ( byteBufferLength );
        kryos.get ( ).writeObject ( output, element );

        return output.toBytes ( );
    }


}
