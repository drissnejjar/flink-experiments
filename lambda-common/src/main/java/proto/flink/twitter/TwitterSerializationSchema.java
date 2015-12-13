
package proto.flink.twitter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class TwitterSerializationSchema implements
    DeserializationSchema<TwitterMessage>, SerializationSchema<TwitterMessage> {

    private static final long serialVersionUID = -8653699115391964472L;

    @Override
    public TypeInformation<TwitterMessage> getProducedType() {
        return TypeInfoParser.parse(TwitterMessage.class.getCanonicalName());
    }

    @Override
    public TwitterMessage deserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in = new ObjectInputStream(bis)) {
            return (TwitterMessage) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public byte[] serialize(TwitterMessage element) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(element);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(TwitterMessage nextElement) {
        return false;
    }
}
