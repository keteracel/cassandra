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
            StorageProxy.mutate(Collections.singletonList(mutation), request.consistencyLevel, Dispatcher.RequestTime.forImmediateExecution());
        }
        catch (RequestExecutionException e)
        {
            return EntryProcessorResponse.writeFailure(e.toString());
        }

        return EntryProcessorResponse.success(result);
    }

    /**
     * Reads the entry's current state locally. This is always a purely local read — see the product spec's
     * decision that the read half of an EntryProcessor invocation needs no consistency level, since execution
     * only ever happens at the key's primary natural replica, which already holds the authoritative local copy.
     */
    private static Row readLocalRow(TableMetadata table, DecoratedKey key)
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
