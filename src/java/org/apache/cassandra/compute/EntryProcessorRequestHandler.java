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
import java.util.Collections;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles {@code Verb.ENTRYPROCESSOR_REQ}: resolves the target table and key, reads the current row locally,
 * runs the requested {@link EntryProcessor}, and hands its delta to Cassandra's own write path.
 * <p>
 * This is also where the technical spec's core claim is realized in code: there is no separate
 * {@code BackupEntryProcessor} dispatch here. {@link #execute} calls {@code StorageProxy.mutate} exactly as any
 * CQL-driven write would — replica resolution, {@code MUTATION_REQ} dispatch to the key's other natural replicas,
 * consistency-level ack counting, and hinted handoff are all unmodified Cassandra machinery, not something this
 * class re-implements.
 * <p>
 * {@link #execute} holds an {@link EntryLocks} per-key lock for the entire read-modify-write, so two concurrent
 * {@link EntryProcessor} invocations for the same key can never interleave their reads and writes — see
 * {@link EntryLocks} for exactly what this guarantee does and does not cover.
 * <p>
 * The mutation is applied to the local replica synchronously (via {@code Mutation.apply()}) before being handed to
 * {@code StorageProxy.mutate}, so the next invocation for this key is always guaranteed to see it once it acquires
 * the lock — {@code StorageProxy.mutate}'s own local application races the other natural replicas' acks to satisfy
 * whatever {@code ConsistencyLevel} the caller requested, and can return before the local apply has actually run
 * under anything short of {@code ALL}. See the inline comment at the call site for the full explanation.
 */
public final class EntryProcessorRequestHandler implements IVerbHandler<EntryProcessorRequest>
{
    public static final EntryProcessorRequestHandler instance = new EntryProcessorRequestHandler();

    private EntryProcessorRequestHandler()
    {
    }

    @Override
    public void doVerb(Message<EntryProcessorRequest> message) throws IOException
    {
        EntryProcessorResponse response = execute(message.payload);
        MessagingService.instance().respond(response, message);
    }

    public static EntryProcessorResponse execute(EntryProcessorRequest request)
    {
        TableMetadata table = Schema.instance.getTableMetadata(request.tableId);
        if (table == null)
            return EntryProcessorResponse.processorFailure("Unknown table: " + request.tableId);

        DecoratedKey key = table.partitioner.decorateKey(request.partitionKey);

        EntryProcessor<ByteBuffer> processor;
        try
        {
            processor = ProcessorRegistry.instantiate(request.processorClassName, request.processorInitArgs);
        }
        catch (ReflectiveOperationException e)
        {
            return EntryProcessorResponse.processorFailure(
                "Could not instantiate processor " + request.processorClassName + ": " + e);
        }

        // Serialize this key's read-modify-write against any other concurrent EntryProcessor invocation for the
        // same key, mirroring CounterMutation's own striped-lock treatment of the same problem shape. See
        // EntryLocks for what this does and does not cover.
        Lock lock = EntryLocks.lockFor(request.tableId, key);
        try
        {
            if (!lock.tryLock(DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS), NANOSECONDS))
                return EntryProcessorResponse.writeFailure("Timed out waiting for exclusive access to key " + key);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return EntryProcessorResponse.writeFailure("Interrupted waiting for exclusive access to key " + key);
        }

        try
        {
            Row currentRow = readLocalRow(table, key);
            SimpleEntryProcessorContext ctx = new SimpleEntryProcessorContext(table.keyspace, table, key, currentRow);

            ByteBuffer result;
            try
            {
                result = processor.process(ctx);
            }
            catch (Exception e)
            {
                return EntryProcessorResponse.processorFailure(
                    "EntryProcessor " + request.processorClassName + " threw: " + e);
            }

            Mutation mutation = ctx.buildMutation();
            try
            {
                // Apply locally first, synchronously, before handing off to StorageProxy. StorageProxy.mutate's
                // own local-replica application (inside performLocally) is submitted asynchronously to the
                // MUTATION stage, and its ack races the other natural replicas' acks to satisfy the requested
                // ConsistencyLevel - under CL.ONE (or anything short of ALL) with more than one replica, mutate()
                // can return as soon as a *remote* replica acks, before our own local apply has actually run. The
                // next EntryProcessor invocation for this key (waiting on EntryLocks right now) must always see
                // this write once it acquires the lock, regardless of what ConsistencyLevel the caller requested
                // for cluster-wide durability - that's an internal correctness requirement of this handler, not
                // something the caller's CL choice should be able to weaken. Re-applying via StorageProxy.mutate
                // below is a harmless idempotent no-op for the local replica (same delta, same timestamp) and is
                // still what performs replication to the other natural replicas at the requested CL.
                mutation.apply();
                StorageProxy.mutate(Collections.singletonList(mutation), request.consistencyLevel, Dispatcher.RequestTime.forImmediateExecution());
            }
            catch (RequestExecutionException e)
            {
                return EntryProcessorResponse.writeFailure(e.toString());
            }

            return EntryProcessorResponse.success(result);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Reads the entry's current state locally. This is always a purely local read — see the product spec's
     * decision that the read half of an EntryProcessor invocation needs no consistency level, since execution
     * only ever happens at the key's primary natural replica, which already holds the authoritative local copy.
     */
    static Row readLocalRow(TableMetadata table, DecoratedKey key)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(table, nowInSec, key, Slices.ALL);
        try (ReadExecutionController controller = command.executionController();
             PartitionIterator partitions = command.executeInternal(controller))
        {
            if (!partitions.hasNext())
                return null;

            try (RowIterator partition = partitions.next())
            {
                return partition.hasNext() ? partition.next() : null;
            }
        }
    }
}
