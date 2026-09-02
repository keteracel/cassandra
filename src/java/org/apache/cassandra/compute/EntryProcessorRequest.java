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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The {@code Verb.ENTRYPROCESSOR_REQ} payload: everything needed to run an {@link EntryProcessor} on the node
 * that receives this message. The processor's own logic never crosses the wire — only a class reference — per
 * the product spec's "processor identity is a class reference, not shipped code" decision. {@code processorInitArgs}
 * is optional, opaque, small init data for processors that need it (empty buffer if none).
 */
public final class EntryProcessorRequest
{
    public final TableId tableId;
    public final ByteBuffer partitionKey;
    public final String processorClassName;
    public final ByteBuffer processorInitArgs;
    public final ConsistencyLevel consistencyLevel;

    public EntryProcessorRequest(TableId tableId,
                                  ByteBuffer partitionKey,
                                  String processorClassName,
                                  ByteBuffer processorInitArgs,
                                  ConsistencyLevel consistencyLevel)
    {
        this.tableId = tableId;
        this.partitionKey = partitionKey;
        this.processorClassName = processorClassName;
        this.processorInitArgs = processorInitArgs;
        this.consistencyLevel = consistencyLevel;
    }

    public static final IVersionedSerializer<EntryProcessorRequest> serializer = new Serializer();

    private static final class Serializer implements IVersionedSerializer<EntryProcessorRequest>
    {
        @Override
        public void serialize(EntryProcessorRequest t, DataOutputPlus out, int version) throws IOException
        {
            t.tableId.serialize(out);
            ByteBufferUtil.writeWithVIntLength(t.partitionKey, out);
            out.writeUTF(t.processorClassName);
            ByteBufferUtil.writeWithVIntLength(t.processorInitArgs, out);
            out.writeUnsignedVInt32(t.consistencyLevel.code);
        }

        @Override
        public EntryProcessorRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            ByteBuffer partitionKey = ByteBufferUtil.readWithVIntLength(in);
            String processorClassName = in.readUTF();
            ByteBuffer processorInitArgs = ByteBufferUtil.readWithVIntLength(in);
            ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readUnsignedVInt32());
            return new EntryProcessorRequest(tableId, partitionKey, processorClassName, processorInitArgs, consistencyLevel);
        }

        @Override
        public long serializedSize(EntryProcessorRequest t, int version)
        {
            long size = t.tableId.serializedSize();
            size += ByteBufferUtil.serializedSizeWithVIntLength(t.partitionKey);
            size += TypeSizes.sizeof(t.processorClassName);
            size += ByteBufferUtil.serializedSizeWithVIntLength(t.processorInitArgs);
            size += TypeSizes.sizeofUnsignedVInt(t.consistencyLevel.code);
            return size;
        }
    }
}
