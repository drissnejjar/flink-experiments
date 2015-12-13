/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package proto.flink;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import proto.flink.twitter.TwitterMessageList;
import proto.flink.twitter.TwitterMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

class QueryableWindowOperator
        extends AbstractStreamOperator<WindowResult<Query, TwitterMessageList>>
        implements TwoInputStreamOperator<TwitterMessage, Query, 
                                    WindowResult<Query, TwitterMessageList>>,
        Triggerable {

    private static final long serialVersionUID = 6412065293508333392L;

    private final Long windowSize;

    private TwitterMessageList state;

    public QueryableWindowOperator(Long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void open() throws Exception {
        super.open();
        state = new TwitterMessageList();

        registerTimer(System.currentTimeMillis() + windowSize, this);
    }

    @Override
    public void processElement1(StreamRecord<TwitterMessage> streamRecord)
            throws Exception {
        state.add(streamRecord.getValue());
    }

    @Override
    public void processElement2(StreamRecord<Query> streamRecord)
            throws Exception {

        output.collect(new StreamRecord<>(
                WindowResult.<TwitterMessageList, Query> queryResult(
                        streamRecord.getValue(), state),
                streamRecord.getTimestamp()));
    }

    @Override
    public void trigger(long l) throws Exception {
        state.clear();
        // TODO: check that no other actions are required here.

        registerTimer(System.currentTimeMillis() + windowSize, this);
    }

    @Override
    public void processWatermark1(Watermark watermark) throws Exception {

    }

    @Override
    public void processWatermark2(Watermark watermark) throws Exception {

    }

    @Override
    public StreamTaskState snapshotOperatorState(long checkpointId,
            long timestamp) throws Exception {
        StreamTaskState taskState = super.snapshotOperatorState(checkpointId,
                timestamp);

        StateBackend.CheckpointStateOutputView out = getStateBackend()
                .createCheckpointStateOutputView(checkpointId, timestamp);

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput outObj = new ObjectOutputStream(bos)) {
            outObj.writeObject(state);
            byte[] data = bos.toByteArray();
            out.writeInt(data.length);
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        taskState.setOperatorState(out.closeAndGetHandle());
        return taskState;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void restoreState(StreamTaskState taskState, long recoveryTimestamp)
            throws Exception {
        super.restoreState(taskState, recoveryTimestamp);

        StateHandle<DataInputView> inputState =
                (StateHandle<DataInputView>) taskState.getOperatorState();
        DataInputView in = inputState.getState(getUserCodeClassloader());

        int dataSize = in.readInt();
        byte[] data = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = in.readByte();
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInput inObj = new ObjectInputStream(bis)) {
            state = (TwitterMessageList) inObj.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
