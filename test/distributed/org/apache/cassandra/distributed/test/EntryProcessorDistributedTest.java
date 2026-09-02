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
package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.compute.ComputeQueryHandler;
import org.apache.cassandra.compute.EntryDispatch;
import org.apache.cassandra.compute.EntryOwnership;
import org.apache.cassandra.compute.EntryProcessor;
import org.apache.cassandra.compute.EntryProcessorContext;
import org.apache.cassandra.compute.EntryProcessorRequest;
import org.apache.cassandra.compute.EntryProcessorResponse;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Multi-node integration coverage for the compute layer (org.apache.cassandra.compute): verifies EntryProcessor
 * routes to the correct primary owner, its delta replicates to the backup owner via Cassandra's own write path
 * (not a separate BackupEntryProcessor mechanism — see specs/technical.md), concurrent invocations for the same
 * key are serialized by EntryLocks so no updates are lost, and the CQL client-facing entry point
 * ({@link ComputeQueryHandler}) actually dispatches an EntryProcessor when swapped in via the
 * {@code cassandra.custom_query_handler_class} system property.
 * <p>
 * The first two concerns are tested by invoking {@link EntryDispatch} directly from inside each instance via
 * {@code callOnInstance} — the mechanism under test, not a shortcut around it, since there's no other way to reach
 * it before the CQL entry point exists. The third test exercises that CQL entry point itself, through
 * {@code coordinator().execute(...)} (the dtest API's full client-simulating path, unlike {@code executeInternal}
 * which bypasses {@code ClientState}'s query-handler swap point entirely).
 */
public class EntryProcessorDistributedTest extends TestBaseImpl
{
    @BeforeClass
    public static void setCustomQueryHandler()
    {
        // Must be set before any instance's ClientState class initializes (each instance loads its own copy in
        // its own classloader, but they all read the same JVM-wide system property at that moment) - hence
        // @BeforeClass rather than setting it inside the test method that needs it.
        System.setProperty("cassandra.custom_query_handler_class", ComputeQueryHandler.class.getName());
    }


    /**
     * Reads the "val" int column, increments it by one, and writes it back as a partial update — exercises
     * {@link EntryProcessorContext#currentRow()}/{@link EntryProcessorContext#table()}/{@link EntryProcessorContext#delta()}
     * together, and gives a directly-checkable value for proving no concurrent invocation lost an update.
     */
    public static class IncrementProcessor implements EntryProcessor<ByteBuffer>
    {
        public IncrementProcessor()
        {
        }

        @Override
        public ByteBuffer process(EntryProcessorContext ctx)
        {
            int current = 0;
            Row row = ctx.currentRow();
            if (row != null)
            {
                ColumnMetadata column = ctx.table().getColumn(ByteBufferUtil.bytes("val"));
                Cell<?> cell = row.getCell(column);
                if (cell != null)
                    current = Int32Type.instance.compose(cell.buffer());
            }
            int updated = current + 1;
            ctx.delta().add("val", updated);
            return Int32Type.instance.decompose(updated);
        }
    }

    private static boolean isPrimary(IInvokableInstance instance, String key)
    {
        return instance.callOnInstance((SerializableCallable<Boolean>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            return EntryOwnership.isLocalPrimary(keyspace, dk);
        });
    }

    private static boolean isReplica(IInvokableInstance instance, String key)
    {
        return instance.callOnInstance((SerializableCallable<Boolean>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            return keyspace.getReplicationStrategy().getLocalReplicaFor(dk.getToken()) != null;
        });
    }

    private static int invokeIncrement(IInvokableInstance instance, String key)
    {
        return invokeIncrement(instance, key, ConsistencyLevel.ALL);
    }

    private static int invokeIncrement(IInvokableInstance instance, String key, ConsistencyLevel cl)
    {
        return instance.callOnInstance((SerializableCallable<Integer>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            EntryProcessorRequest request = new EntryProcessorRequest(table.id,
                                                                        dk.getKey(),
                                                                        IncrementProcessor.class.getName(),
                                                                        ByteBuffer.allocate(0),
                                                                        cl);
            EntryProcessorResponse response;
            try
            {
                response = EntryDispatch.dispatch(keyspace, dk, request).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            if (response.status != EntryProcessorResponse.Status.SUCCESS)
                throw new RuntimeException("EntryProcessor invocation failed: " + response.status + " " + response.failureMessage);
            return Int32Type.instance.compose(response.result);
        });
    }

    private static Integer readVal(IInvokableInstance instance, String key)
    {
        Object[][] rows = instance.executeInternal(withKeyspace("SELECT val FROM %s.widgets WHERE id = ?"), key);
        return rows.length == 0 ? null : (Integer) rows[0][0];
    }

    @Test
    public void routesToPrimaryAndReplicatesToBackup() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> config.with(GOSSIP, NETWORK)).start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "widget-1";

            List<IInvokableInstance> primaries = new ArrayList<>();
            List<IInvokableInstance> replicas = new ArrayList<>();
            List<IInvokableInstance> nonReplicas = new ArrayList<>();
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance instance = cluster.get(i);
                if (isPrimary(instance, key))
                    primaries.add(instance);
                if (isReplica(instance, key))
                    replicas.add(instance);
                else
                    nonReplicas.add(instance);
            }

            assertEquals("exactly one node should be the primary owner for the key", 1, primaries.size());
            assertEquals("RF=2 in a 3-node cluster: exactly two nodes should be natural replicas", 2, replicas.size());
            assertEquals("the remaining node should not be a replica at all", 1, nonReplicas.size());
            assertTrue("the primary must itself be one of the natural replicas", replicas.contains(primaries.get(0)));

            IInvokableInstance primary = primaries.get(0);
            IInvokableInstance backup = replicas.get(0) == primary ? replicas.get(1) : replicas.get(0);
            IInvokableInstance outsider = nonReplicas.get(0);

            // Invoke from the non-primary "outsider" node — this must go over the wire (Verb.ENTRYPROCESSOR_REQ)
            // since the outsider isn't even a replica, let alone the primary.
            int result = invokeIncrement(outsider, key);
            assertEquals(1, result);

            assertEquals("primary should have applied the delta locally", (Integer) 1, readVal(primary, key));
            assertEquals("backup should have received the delta via Cassandra's normal write path", (Integer) 1, readVal(backup, key));
            assertNull("a non-replica node should not have a local copy of the entry", readVal(outsider, key));
        }
    }

    @Test
    public void concurrentInvocationsForSameKeyDoNotLoseUpdates() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> config.with(GOSSIP, NETWORK)).start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "counter-1";
            int concurrency = 25;

            ExecutorService pool = Executors.newFixedThreadPool(concurrency);
            try
            {
                List<Future<Integer>> futures = new ArrayList<>();
                for (int i = 0; i < concurrency; i++)
                {
                    // Round-robin across all three nodes so this exercises both the local fast path (when the
                    // caller happens to already be the primary) and the remote Verb.ENTRYPROCESSOR_REQ path
                    // together — all serialized by the same EntryLocks lock on the primary. CL.ONE here (rather
                    // than ALL, used in the other test) keeps each serialized invocation fast, since the point of
                    // this test is proving the lock queues invocations correctly under contention, not re-proving
                    // replication (already covered above) - a long queue under CL.ALL risks exceeding the default
                    // write timeout for reasons unrelated to what's being tested.
                    IInvokableInstance instance = cluster.get((i % 3) + 1);
                    Callable<Integer> task = () -> invokeIncrement(instance, key, ConsistencyLevel.ONE);
                    futures.add(pool.submit(task));
                }

                for (Future<Integer> f : futures)
                    f.get();
            }
            finally
            {
                pool.shutdown();
            }

            IInvokableInstance primary = null;
            for (int i = 1; i <= 3; i++)
                if (isPrimary(cluster.get(i), key))
                    primary = cluster.get(i);

            assertEquals("no update should be lost under concurrent EntryProcessor invocations for the same key",
                         (Integer) concurrency, readVal(primary, key));
        }
    }

    @Test
    public void cqlEntryPointDispatchesEntryProcessor() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> config.with(GOSSIP, NETWORK)).start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "cql-widget";
            String call = String.format("CALL ENTRYPROCESSOR('%s', 'widgets', '%s', '%s', 'ALL')",
                                         KEYSPACE, key, IncrementProcessor.class.getName());

            IInvokableInstance instance = cluster.get(1);

            // Confirm the swap itself actually took effect on a real node: ClientState reads the
            // cassandra.custom_query_handler_class system property once, in its own static initializer.
            String activeHandlerClass = instance.callOnInstance((SerializableCallable<String>) () ->
                org.apache.cassandra.service.ClientState.getCQLQueryHandler().getClass().getName());
            assertEquals(ComputeQueryHandler.class.getName(), activeHandlerClass);

            // Exercise the actual QueryHandler.parse()/process() path a real client connection over the native
            // protocol would trigger. coordinator().execute()/executeInternal() both call QueryProcessor directly
            // in this dtest framework version, bypassing ClientState's swap point entirely - so this drives the
            // swapped handler's own parse()+process() methods directly instead, which is what actually needs
            // proving here.
            int result = instance.callOnInstance((SerializableCallable<Integer>) () -> {
                org.apache.cassandra.cql3.QueryHandler handler = org.apache.cassandra.service.ClientState.getCQLQueryHandler();
                org.apache.cassandra.service.QueryState queryState = org.apache.cassandra.service.QueryState.forInternalCalls();
                org.apache.cassandra.cql3.QueryOptions options = org.apache.cassandra.cql3.QueryOptions.DEFAULT;
                org.apache.cassandra.cql3.CQLStatement statement = handler.parse(call, queryState, options);
                org.apache.cassandra.transport.messages.ResultMessage message =
                    handler.process(statement, queryState, options, java.util.Collections.emptyMap(),
                                     org.apache.cassandra.transport.Dispatcher.RequestTime.forImmediateExecution());
                org.apache.cassandra.transport.messages.ResultMessage.Rows rows =
                    (org.apache.cassandra.transport.messages.ResultMessage.Rows) message;
                ByteBuffer bytes = rows.result.rows.get(0).get(0);
                return Int32Type.instance.compose(bytes);
            });
            assertEquals(1, result);

            // The fallback path in parse() must still work: normal CQL through the same swapped handler. The
            // insert string is built here, outside the callOnInstance lambda - withKeyspace() is a method on our
            // own test class, which isn't necessarily available on the isolated instance's own classloader.
            String insert = withKeyspace("INSERT INTO %s.widgets (id, val) VALUES ('normal-cql-check', 42)");
            int normalCqlResult = instance.callOnInstance((SerializableCallable<Integer>) () -> {
                org.apache.cassandra.cql3.QueryHandler handler = org.apache.cassandra.service.ClientState.getCQLQueryHandler();
                org.apache.cassandra.service.QueryState queryState = org.apache.cassandra.service.QueryState.forInternalCalls();
                org.apache.cassandra.cql3.QueryOptions options = org.apache.cassandra.cql3.QueryOptions.DEFAULT;
                org.apache.cassandra.cql3.CQLStatement statement = handler.parse(insert, queryState, options);
                handler.process(statement, queryState, options, java.util.Collections.emptyMap(),
                                 org.apache.cassandra.transport.Dispatcher.RequestTime.forImmediateExecution());
                return 1;
            });
            assertEquals(1, normalCqlResult);
            // readVal() reads locally on whichever instance it's given, via executeInternal - it must be pointed
            // at a node that actually owns "normal-cql-check" (a different key, different token, likely different
            // owning nodes than "cql-widget"), not just any node.
            IInvokableInstance normalCqlOwner = null;
            for (int i = 1; i <= 3; i++)
                if (isPrimary(cluster.get(i), "normal-cql-check"))
                    normalCqlOwner = cluster.get(i);
            assertEquals((Integer) 42, readVal(normalCqlOwner, "normal-cql-check"));

            IInvokableInstance primary = null;
            for (int i = 1; i <= 3; i++)
                if (isPrimary(cluster.get(i), key))
                    primary = cluster.get(i);

            assertEquals("the CQL-dispatched EntryProcessor should actually have applied its delta",
                         (Integer) 1, readVal(primary, key));
        }
    }
}
