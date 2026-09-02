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

import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

/**
 * The single place that decides, for a given key, whether {@link EntryProcessor} execution happens in-process or
 * over the wire — the "local execution bypasses serialization" decision from the product spec.
 * <p>
 * This mirrors {@code StorageProxy.appliesLocally}/{@code performLocally}: when the current node is already the
 * key's primary (per {@link EntryOwnership}), {@link #dispatch} calls {@link EntryProcessorRequestHandler#execute}
 * directly — no {@link Message} is built and {@link EntryProcessorRequest}'s serializer is never invoked. Only a
 * genuinely remote invocation goes through {@link MessagingService}.
 */
public final class EntryDispatch
{
    private EntryDispatch()
    {
    }

    /**
     * Dispatches a wire-shaped request: runs it in-process if this node is already the key's primary, otherwise
     * sends it to the primary via {@code Verb.ENTRYPROCESSOR_REQ}. Only {@code EntryProcessor<ByteBuffer>}
     * implementations can go through this path, since {@code request} is what would be serialized if this turns
     * out not to be local.
     */
    public static Future<EntryProcessorResponse> dispatch(Keyspace keyspace, DecoratedKey key, EntryProcessorRequest request)
    {
        if (EntryOwnership.isLocalPrimary(keyspace, key))
            return ImmediateFuture.success(EntryProcessorRequestHandler.execute(request));

        Replica primary = EntryOwnership.primary(keyspace, key);
        Message<EntryProcessorRequest> message = Message.out(Verb.ENTRYPROCESSOR_REQ, request);
        return MessagingService.instance()
                                .<EntryProcessorRequest, EntryProcessorResponse>sendWithResult(message, primary.endpoint())
                                .map(response -> response.payload);
    }

    /**
     * Invokes an already-in-hand {@link EntryProcessor} directly, for callers that know execution is local and
     * have no need to go through the {@link EntryProcessorRequest}/class-reference indirection at all — the
     * fastest path, with an unrestricted result type {@code R} since nothing here is ever serialized.
     * <p>
     * Callers are responsible for having already confirmed locality via {@link EntryOwnership#isLocalPrimary};
     * this method does not check it, since re-checking here would defeat the point of exposing it separately from
     * {@link #dispatch}. The read of the entry's current state happens inside this method (not before it), so the
     * {@link EntryLocks} lock below covers the whole read-modify-write, exactly as {@link EntryProcessorRequestHandler#execute}
     * does — a caller pre-reading the row itself would open the same race this lock exists to close.
     */
    public static <R> R invokeLocally(String keyspaceName,
                                       TableMetadata table,
                                       DecoratedKey key,
                                       EntryProcessor<R> processor,
                                       ConsistencyLevel consistencyLevel) throws Exception
    {
        Lock lock = EntryLocks.lockFor(table.id, key);
        if (!lock.tryLock(DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS), NANOSECONDS))
            throw new TimeoutException("Timed out waiting for exclusive access to key " + key);

        try
        {
            Row currentRow = EntryProcessorRequestHandler.readLocalRow(table, key);
            SimpleEntryProcessorContext ctx = new SimpleEntryProcessorContext(keyspaceName, table, key, currentRow);
            R result = processor.process(ctx);
            Mutation mutation = ctx.buildMutation();
            // Apply locally first, synchronously - see the comment at the equivalent call site in
            // EntryProcessorRequestHandler.execute() for why StorageProxy.mutate() alone doesn't guarantee this.
            mutation.apply();
            StorageProxy.mutate(Collections.singletonList(mutation), consistencyLevel, Dispatcher.RequestTime.forImmediateExecution());
            return result;
        }
        finally
        {
            lock.unlock();
        }
    }
}
