/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.compute;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The {@code Verb.ENTRYPROCESSOR_RSP} payload. Distinguishes a processor exception from a write/consistency
 * failure, per the product spec's acceptance criterion that the two must be distinguishable in the API — they
 * mean different things: {@link Status#PROCESSOR_FAILURE} means the processor ran and threw; {@link
 * Status#WRITE_FAILURE} means the processor's result was computed but its delta didn't meet the requested
 * {@code ConsistencyLevel} (mirroring {@code StorageProxy.mutate}'s own exception contract).
 * <p>
 * {@code result} carries a processor's return value pre-serialized to bytes by the processor itself; this is a
 * deliberate v1 scope limit — only {@code EntryProcessor<ByteBuffer>} implementations can be dispatched over the
 * wire (see {@link EntryProcessorRequestHandler}). A processor invoked through the local fast path is not subject
 * to this restriction, since no serialization happens on that path at all.
 */
public final class EntryProcessorResponse
{
    public enum Status
    {
        SUCCESS, PROCESSOR_FAILURE, WRITE_FAILURE
    }

    public final Status status;
    public final ByteBuffer result;
    public final String failureMessage;

    private EntryProcessorResponse(Status status, ByteBuffer result, String failureMessage)
    {
        this.status = status;
        this.result = result;
        this.failureMessage = failureMessage;
    }

    public static EntryProcessorResponse success(ByteBuffer result)
    {
        return new EntryProcessorResponse(Status.SUCCESS, result, null);
    }

    public static EntryProcessorResponse processorFailure(String message)
    {
        return new EntryProcessorResponse(Status.PROCESSOR_FAILURE, null, message);
    }

    public static EntryProcessorResponse writeFailure(String message)
    {
        return new EntryProcessorResponse(Status.WRITE_FAILURE, null, message);
    }

    public static final IVersionedSerializer<EntryProcessorResponse> serializer = new Serializer();

    private static final class Serializer implements IVersionedSerializer<EntryProcessorResponse>
    {
        @Override
        public void serialize(EntryProcessorResponse t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.status.ordinal());
            switch (t.status)
            {
                case SUCCESS:
                    ByteBufferUtil.writeWithVIntLength(t.result, out);
                    break;
                case PROCESSOR_FAILURE:
                case WRITE_FAILURE:
                    out.writeUTF(t.failureMessage);
                    break;
            }
        }

        @Override
        public EntryProcessorResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            Status status = Status.values()[in.readByte()];
            switch (status)
            {
                case SUCCESS:
                    return success(ByteBufferUtil.readWithVIntLength(in));
                case PROCESSOR_FAILURE:
                    return processorFailure(in.readUTF());
                case WRITE_FAILURE:
                    return writeFailure(in.readUTF());
                default:
                    throw new IOException("Unknown EntryProcessorResponse status: " + status);
            }
        }

        @Override
        public long serializedSize(EntryProcessorResponse t, int version)
        {
            long size = TypeSizes.sizeof((byte) t.status.ordinal());
            switch (t.status)
            {
                case SUCCESS:
                    size += ByteBufferUtil.serializedSizeWithVIntLength(t.result);
                    break;
                case PROCESSOR_FAILURE:
                case WRITE_FAILURE:
                    size += TypeSizes.sizeof(t.failureMessage);
                    break;
            }
            return size;
        }
    }
}
