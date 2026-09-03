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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Round-trip tests for {@link EntryProcessorRequest.serializer}/{@link EntryProcessorResponse.serializer}
 * ({@code IVersionedSerializer}s for {@code Verb.ENTRYPROCESSOR_REQ}/{@code ENTRYPROCESSOR_RSP}).
 * <p>
 * Before this test, both serializers were only ever exercised indirectly, as a side effect of multi-node dtests
 * sending real messages over the wire — and even then, only the {@code SUCCESS} and {@code PROCESSOR_FAILURE}
 * {@link EntryProcessorResponse.Status} variants were ever actually produced by any test. {@code WRITE_FAILURE} -
 * only reachable when a remotely-dispatched processor runs successfully but then fails to replicate its delta at
 * the requested consistency level - had never been serialized/deserialized by anything before this test (see
 * {@link EntryProcessorRemoteWriteFailureTest} for the corresponding live, over-the-wire proof).
 */
public class EntryProcessorSerializationTest
{
    private static final int VERSION = MessagingService.current_version;

    @Test
    public void requestRoundTrips() throws Exception
    {
        EntryProcessorRequest request = new EntryProcessorRequest(TableId.generate(),
                                                                    ByteBufferUtil.bytes("some-partition-key"),
                                                                    "com.example.SomeProcessor",
                                                                    ByteBufferUtil.bytes("small-init-args"),
                                                                    ConsistencyLevel.QUORUM);

        EntryProcessorRequest deserialized = roundTrip(request, EntryProcessorRequest.serializer);

        assertEquals(request.tableId, deserialized.tableId);
        assertEquals(request.partitionKey, deserialized.partitionKey);
        assertEquals(request.processorClassName, deserialized.processorClassName);
        assertEquals(request.processorInitArgs, deserialized.processorInitArgs);
        assertEquals(request.consistencyLevel, deserialized.consistencyLevel);
    }

    @Test
    public void requestRoundTripsWithEmptyInitArgs() throws Exception
    {
        // The shape every real caller actually sends today - EntryProcessorCallStatement always passes
        // ByteBufferUtil.EMPTY_BYTE_BUFFER, since there's no CQL syntax to supply non-empty init args.
        EntryProcessorRequest request = new EntryProcessorRequest(TableId.generate(),
                                                                    ByteBufferUtil.bytes("k"),
                                                                    "com.example.SomeProcessor",
                                                                    ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                    ConsistencyLevel.ONE);

        EntryProcessorRequest deserialized = roundTrip(request, EntryProcessorRequest.serializer);

        assertEquals(request.processorInitArgs, deserialized.processorInitArgs);
        assertEquals(0, deserialized.processorInitArgs.remaining());
    }

    @Test
    public void responseRoundTripsSuccess() throws Exception
    {
        EntryProcessorResponse response = EntryProcessorResponse.success(ByteBufferUtil.bytes("the-result"));

        EntryProcessorResponse deserialized = roundTrip(response, EntryProcessorResponse.serializer);

        assertEquals(EntryProcessorResponse.Status.SUCCESS, deserialized.status);
        assertEquals(response.result, deserialized.result);
        assertNull(deserialized.failureMessage);
    }

    @Test
    public void responseRoundTripsProcessorFailure() throws Exception
    {
        EntryProcessorResponse response = EntryProcessorResponse.processorFailure(
            "EntryProcessor com.example.SomeProcessor threw: java.lang.IllegalStateException: boom");

        EntryProcessorResponse deserialized = roundTrip(response, EntryProcessorResponse.serializer);

        assertEquals(EntryProcessorResponse.Status.PROCESSOR_FAILURE, deserialized.status);
        assertEquals(response.failureMessage, deserialized.failureMessage);
        assertNull(deserialized.result);
    }

    /**
     * The variant that, before this test, had never actually been serialized or deserialized by anything - see
     * this class's javadoc.
     */
    @Test
    public void responseRoundTripsWriteFailure() throws Exception
    {
        EntryProcessorResponse response = EntryProcessorResponse.writeFailure(
            "org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level ALL");

        EntryProcessorResponse deserialized = roundTrip(response, EntryProcessorResponse.serializer);

        assertEquals(EntryProcessorResponse.Status.WRITE_FAILURE, deserialized.status);
        assertEquals(response.failureMessage, deserialized.failureMessage);
        assertNull(deserialized.result);
    }

    private static <T> T roundTrip(T value, org.apache.cassandra.io.IVersionedSerializer<T> serializer) throws Exception
    {
        ByteBuffer buffer;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(value, out, VERSION);
            buffer = out.buffer();
        }

        // Pins down that serializedSize() isn't just a plausible-looking guess: a mismatch here (too small) would
        // mean real message framing under-allocates its buffer, and (too large) would mean wasted allocation on
        // every real send - neither was ever checked before this test.
        assertEquals("serializedSize() must exactly match what serialize() actually wrote",
                     buffer.remaining(), serializer.serializedSize(value, VERSION));

        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            return serializer.deserialize(in, VERSION);
        }
    }
}
