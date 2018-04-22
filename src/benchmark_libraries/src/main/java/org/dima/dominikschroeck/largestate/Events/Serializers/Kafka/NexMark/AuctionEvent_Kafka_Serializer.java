package org.dima.dominikschroeck.largestate.Events.Serializers.Kafka.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark.AuctionEvent_Kryo_Serializer;

import java.util.Map;


public class AuctionEvent_Kafka_Serializer implements Serializer<AuctionEvent>, Deserializer<AuctionEvent> {


    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( AuctionEvent.class, new AuctionEvent_Kryo_Serializer ( ) );
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
    public AuctionEvent deserialize(String topic, byte[] message) {
        AuctionEvent out = kryos.get ( ).readObject ( new ByteBufferInput ( message ), AuctionEvent.class );
        out.setIngestion_timestamp ( System.currentTimeMillis ( ) );
        return out;
    }


    @Override
    public byte[] serialize(String topic, AuctionEvent element) {
        int byteBufferLength = 64 / 8 + 32 / 8 + 32 / 8 + 32 / 8 + 64 / 8 + 64 / 8 + 64 / 8 + 64 / 8 + 1;

        ByteBufferOutput output = new ByteBufferOutput ( byteBufferLength );
        kryos.get ( ).writeObject ( output, element );

        return output.toBytes ( );
    }


}
