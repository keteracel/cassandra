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

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.compute.EntryDispatch;
import org.apache.cassandra.compute.EntryOwnership;
import org.apache.cassandra.compute.EntryProcessorRequest;
import org.apache.cassandra.compute.EntryProcessorResponse;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Product spec acceptance criterion: "{@code EntryProcessor} submission accepts a {@code ConsistencyLevel} and
 * exhibits the same ack-counting, timeout, and hinted-handoff behavior as an equivalent plain Cassandra write at
 * that level (verifiable by killing a replica and confirming behavior matches a normal CQL write at the same CL)."
 * Before this test, nothing exercised this: every existing CL-aware test either used {@code ALL} with every node
 * healthy, or exercised the *primary* being entirely unreachable (a different failure mode, dispatch-level rather
 * than replication-level - see {@code EntryProcessorRemoteWriteFailureTest} for that one).
 * <p>
 * This shuts down one of a key's two natural replicas (the "backup", not the primary) and compares, at the same
 * consistency levels, a plain CQL write against an {@code EntryProcessor} submission for the *same key* (so both
 * share the exact same natural-replica set - not just "a similarly-configured key"):
 * <ul>
 *     <li>{@code CL.ALL}: both should fail, since only 1 of the 2 required replicas is live.</li>
 *     <li>{@code CL.ONE}: both should succeed (the primary alone satisfies it), and both should record a hint
 *     for the down backup.</li>
 * </ul>
 * and finally that the hint delivers once the backup comes back, bringing it into eventual agreement with the
 * primary - scoped down from fully predicting the exact delivered value (fragile to pin down given write-timestamp
 * ordering between the plain write and the EntryProcessor's own read-modify-write) to confirming eventual
 * agreement between the two, which is the property that actually matters here.
 */
public class EntryProcessorConsistencyParityTest extends TestBaseImpl
{
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

    /** Same "STATUS|message" trick as EntryLocksContentionTest - EntryProcessorResponse can't safely cross the
     *  dtest classloader boundary since it isn't Serializable. */
    private static String invokeAndDescribeStatus(IInvokableInstance instance, String key, ConsistencyLevel cl, int value)
    {
        return instance.callOnInstance((SerializableCallable<String>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            EntryProcessorRequest request = new EntryProcessorRequest(table.id,
                                                                        dk.getKey(),
                                                                        SetValueProcessor.class.getName(),
                                                                        org.apache.cassandra.utils.ByteBufferUtil.bytes(value),
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
            return response.status.name() + "|" + response.failureMessage;
        });
    }

    @SuppressWarnings("Convert2MethodRef")
    private static long countTotalHints(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StorageMetrics.totalHints.getCount());
    }

    private static Integer readVal(IInvokableInstance instance, String key)
    {
        Object[][] rows = instance.executeInternal(withKeyspace("SELECT val FROM %s.widgets WHERE id = ?"), key);
        return rows.length == 0 ? null : (Integer) rows[0][0];
    }

    /**
     * Takes its new value from {@code processorInitArgs} (an {@code int}, via the {@code (ByteBuffer)} constructor
     * - see {@code ProcessorRegistryTest} for that path's own coverage) rather than reading-and-incrementing, so
     * this test can set a specific, known value each time without depending on whatever the current row holds -
     * simpler to reason about than {@code IncrementProcessor} for a test that's already juggling two CLs and two
     * write mechanisms.
     */
    public static class SetValueProcessor implements org.apache.cassandra.compute.EntryProcessor<ByteBuffer>
    {
        private final int value;

        public SetValueProcessor(ByteBuffer initArgs)
        {
            this.value = initArgs.duplicate().getInt();
        }

        @Override
        public ByteBuffer process(org.apache.cassandra.compute.EntryProcessorContext ctx)
        {
            ctx.delta().add("val", value);
            return org.apache.cassandra.db.marshal.Int32Type.instance.decompose(value);
        }
    }

    @Test
    public void entryProcessorMatchesPlainWriteBehaviorWhenABackupReplicaIsDown() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> config.with(GOSSIP, NETWORK)).start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "parity-key";

            List<IInvokableInstance> replicas = new ArrayList<>();
            IInvokableInstance primary = null;
            IInvokableInstance outsider = null;
            int outsiderNum = -1;
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance instance = cluster.get(i);
                if (isPrimary(instance, key))
                    primary = instance;
                if (isReplica(instance, key))
                    replicas.add(instance);
                else
                {
                    outsider = instance;
                    outsiderNum = i;
                }
            }
            assertEquals("RF=2 in a 3-node cluster: exactly two nodes should be natural replicas", 2, replicas.size());
            IInvokableInstance backup = replicas.get(0) == primary ? replicas.get(1) : replicas.get(0);
            assertTrue("test setup: need a distinct primary, backup, and outsider",
                       primary != null && outsider != null && primary != backup && outsider != primary && outsider != backup);
            // primary/outsider are reassigned inside the loop above, so neither is effectively final - lambdas
            // below need stable references to capture.
            final IInvokableInstance primaryRef = primary;
            final IInvokableInstance outsiderRef = outsider;

            backup.shutdown().get();
            // Give gossip/the failure detector a moment to actually mark the backup down on the other nodes -
            // without this, a write issued immediately after shutdown() can race ahead of that propagation and
            // see different (slower, timeout-based rather than immediate-Unavailable) failure behavior than what
            // this test is actually trying to compare - both plain writes and EntryProcessor submissions consult
            // the same failure detector state, so this affects both sides identically rather than skewing the
            // comparison.
            Thread.sleep(1000);

            // --- ConsistencyLevel.ALL: both should fail, since only 1 of the 2 required replicas is live. ---

            boolean plainWriteFailedAtAll;
            try
            {
                cluster.coordinator(outsiderNum)
                       .execute(withKeyspace("INSERT INTO %s.widgets (id, val) VALUES (?, ?)"),
                                org.apache.cassandra.distributed.api.ConsistencyLevel.ALL, key, 111);
                plainWriteFailedAtAll = false;
            }
            catch (Throwable t)
            {
                plainWriteFailedAtAll = true;
            }
            assertTrue("expected a plain write at CL.ALL to fail with one of two replicas down", plainWriteFailedAtAll);

            String epResultAtAll = invokeAndDescribeStatus(outsider, key, ConsistencyLevel.ALL, 222);
            assertTrue("expected the EntryProcessor submission at CL.ALL to also fail (WRITE_FAILURE), got: " + epResultAtAll,
                       epResultAtAll.startsWith("WRITE_FAILURE|"));

            // --- ConsistencyLevel.ONE: both should succeed, and both should record a hint for the down backup. ---

            // The plain write's hint is recorded by whichever node *coordinates* it - here, outsider (since it's
            // the one calling StorageProxy.mutate against the down backup on the real write path) - not primary.
            long hintsBeforePlainWrite = countTotalHints(outsider);
            cluster.coordinator(outsiderNum)
                   .execute(withKeyspace("INSERT INTO %s.widgets (id, val) VALUES (?, ?)"),
                            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE, key, 333);
            Awaitility.await().atMost(10, SECONDS).until(() -> countTotalHints(outsiderRef) > hintsBeforePlainWrite);
            assertEquals("plain write at CL.ONE should have applied", (Integer) 333, readVal(primary, key));

            long hintsBeforeEpWrite = countTotalHints(primary);
            String epResultAtOne = invokeAndDescribeStatus(outsider, key, ConsistencyLevel.ONE, 444);
            assertEquals("expected the EntryProcessor submission at CL.ONE to succeed", "SUCCESS|null", epResultAtOne);
            Awaitility.await().atMost(10, SECONDS).until(() -> countTotalHints(primaryRef) > hintsBeforeEpWrite);
            assertEquals("EntryProcessor delta at CL.ONE should have applied", (Integer) 444, readVal(primary, key));

            // --- Bring the backup back and confirm the hint(s) eventually deliver, converging with the primary. ---

            backup.startup();
            Integer primaryFinalValue = readVal(primary, key);
            Awaitility.await().atMost(30, SECONDS).pollDelay(2, SECONDS)
                      .until(() -> primaryFinalValue.equals(readVal(backup, key)));
        }
    }
}
