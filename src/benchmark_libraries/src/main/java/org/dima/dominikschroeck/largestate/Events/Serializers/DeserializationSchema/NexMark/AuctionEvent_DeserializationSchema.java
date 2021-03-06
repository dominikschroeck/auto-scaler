package org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.NexMark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;

import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.dima.dominikschroeck.largestate.Events.NexMark.AuctionEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.NexMark.AuctionEvent_Kryo_Serializer;

import java.io.IOException;

public class AuctionEvent_DeserializationSchema extends AbstractDeserializationSchema<AuctionEvent> {


    private transient ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( AuctionEvent.class, new AuctionEvent_Kryo_Serializer ( ) );
            return kryo;
        }
    };


    @Override
    public AuctionEvent deserialize(byte[] message) throws IOException {

        Serializer<AuctionEvent> serializer = new AuctionEvent_Kryo_Serializer ( );
        Kryo kryo = new Kryo ( );
        kryo.addDefaultSerializer ( AuctionEvent.class, serializer );

        DataInputDeserializer dis = new DataInputDeserializer ( message, 0, message.length );
        NoFetchingInput noFetchingInput = new NoFetchingInput ( new DataInputViewStream ( dis ) );


        AuctionEvent out = serializer.read ( kryo, new Input ( new DataInputViewStream ( dis ) ), AuctionEvent.class );


        return out;
    }


    /**@Override public byte[] serialize(PSM_SearchEvent element) {
    ByteBufferOutput output = new ByteBufferOutput (byteBufferLength);
    kryos.get().writeObject(output, element);
    return output.toBytes();
    }*/

}
