package org.dima.dominikschroeck.largestate.Events.Serializers.DeserializationSchema.PSM;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import org.apache.flink.api.common.ExecutionConfig;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;


import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_ClickEvent;
import org.dima.dominikschroeck.largestate.Events.PSM.PSM_SearchEvent;
import org.dima.dominikschroeck.largestate.Events.Serializers.Kryo.Kafka.PSM.ClickEvent_Kryo_Serializer;

import java.io.IOException;

//import org.apache.flink.core.memory.DataInputDeserializer;

public class ClickEvent_DeserializationSchema implements DeserializationSchema<PSM_ClickEvent>, SerializationSchema<PSM_ClickEvent> {


    private transient ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo> ( ) {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo ( );
            kryo.addDefaultSerializer ( PSM_SearchEvent.class, new ClickEvent_Kryo_Serializer ( ) );
            return kryo;
        }
    };


    private transient ExecutionConfig cfg;

    public ClickEvent_DeserializationSchema(ExecutionConfig cfg) {
        this.cfg = cfg;
    }


    @Override
    public PSM_ClickEvent deserialize(byte[] message) throws IOException {

        Serializer<PSM_ClickEvent> serializer = new ClickEvent_Kryo_Serializer ( );
        Kryo kryo = new Kryo ( );
        kryo.addDefaultSerializer ( PSM_ClickEvent.class, serializer );

        DataInputDeserializer dis = new DataInputDeserializer ( message, 0, message.length );
        NoFetchingInput noFetchingInput = new NoFetchingInput ( new DataInputViewStream ( dis ) );


        PSM_ClickEvent out = serializer.read ( kryo, new Input ( new DataInputViewStream ( dis ) ), PSM_ClickEvent.class );

        out.setIngestion_stamp ( System.currentTimeMillis ( ) );
        return out;
    }

    @Override
    public boolean isEndOfStream(PSM_ClickEvent nextElement) {
        return false;
    }


    @Override
    public byte[] serialize(PSM_ClickEvent element) {
        int byteBufferLength = 64 / 8 + element.getCategory ( ).length ( ) + element.getProduct ( ).length ( ) + element.getOwner ( ).length ( ) + 64 / 8 + 1;
        ByteBufferOutput output = new ByteBufferOutput ( byteBufferLength );
        kryos.get ( ).writeObject ( output, element );
        return output.toBytes ( );
    }

    @Override
    public TypeInformation<PSM_ClickEvent> getProducedType() {
        return TypeExtractor.getForClass ( PSM_ClickEvent.class );
    }
}
