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

import org.junit.Test;

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
 * (not a separate BackupEntryProcessor mechanism — see specs/technical.md), and concurrent invocations for the
 * same key are serialized by EntryLocks so no updates are lost.
 * <p>
 * There is no client-facing entry point yet (the CQL QueryHandler is a later implementation-plan step), so this
 * test invokes {@link EntryDispatch} directly from inside each instance via {@code callOnInstance} — which is
 * exactly the mechanism under test, not a shortcut around it.
 */
public class EntryProcessorDistributedTest extends TestBaseImpl
{
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
}
