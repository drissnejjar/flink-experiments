package proto.flink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class ResultSerializationSchema implements
    DeserializationSchema<Tuple2<Query, String>>,
    SerializationSchema<Tuple2<Query, String>> {

    private static final long serialVersionUID = 8963916997808927635L;

    @Override
    public TypeInformation<Tuple2<Query, String>> getProducedType() {
        TupleTypeInfo<Tuple2<Query, String>> resultType =
                new TupleTypeInfo<>(
                        TypeInfoParser.parse(Query.class.getCanonicalName()),
                        BasicTypeInfo.STRING_TYPE_INFO);
        return resultType;
    }

    @Override
    public byte[] serialize(Tuple2<Query, String> arg0) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(arg0);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple2<Query, String> deserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in = new ObjectInputStream(bis)) {
            return (Tuple2<Query, String>) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Tuple2<Query, String> arg0) {
        return false;
    }

}
