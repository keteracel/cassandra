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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Proves {@code EntryProcessorResponse.Status.WRITE_FAILURE} actually works over the wire (real
 * {@code Verb.ENTRYPROCESSOR_REQ}/{@code ENTRYPROCESSOR_RSP} serialization, not just synthesized in a unit test -
 * see {@code EntryProcessorSerializationTest} for that half) - previously this status had never been produced by
 * any test at all: {@code EntryProcessorDistributedTest.routesToPrimaryAndReplicatesToBackup} uses
 * {@code ConsistencyLevel.ALL} with every node healthy (never fails), and
 * {@code primaryUnreachableFailsClearlyWithinBoundedTime} fails via a dispatch-level
 * {@code MessagingService.FailureResponseException} before a response is ever built at all - a fundamentally
 * different failure mode from this one, where the *primary* is reachable and runs the processor successfully, but
 * replicating its delta to the *other* replica fails to meet the requested consistency level.
 * <p>
 * This drives {@link EntryDispatch} directly, the same way {@code EntryProcessorDistributedTest} does, since
 * there's no other way to reach the compute layer before/without the CQL entry point.
 */
public class EntryProcessorRemoteWriteFailureTest extends TestBaseImpl
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

    private static void invokeIncrement(IInvokableInstance instance, String key, ConsistencyLevel cl)
    {
        instance.callOnInstance((SerializableCallable<Integer>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            EntryProcessorRequest request = new EntryProcessorRequest(table.id,
                                                                        dk.getKey(),
                                                                        EntryProcessorDistributedTest.IncrementProcessor.class.getName(),
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
            return 0;
        });
    }

    @Test
    public void remoteDispatchReportsWriteFailureWhenReplicationCantMeetRequestedCL() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> config.with(GOSSIP, NETWORK)).start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "remote-write-failure-key";

            List<IInvokableInstance> replicas = new ArrayList<>();
            List<IInvokableInstance> nonReplicas = new ArrayList<>();
            IInvokableInstance primary = null;
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance instance = cluster.get(i);
                if (isPrimary(instance, key))
                    primary = instance;
                if (isReplica(instance, key))
                    replicas.add(instance);
                else
                    nonReplicas.add(instance);
            }
            assertEquals("RF=2 in a 3-node cluster: exactly two nodes should be natural replicas", 2, replicas.size());
            assertEquals("the remaining node should not be a replica at all", 1, nonReplicas.size());

            IInvokableInstance backup = replicas.get(0) == primary ? replicas.get(1) : replicas.get(0);
            IInvokableInstance outsider = nonReplicas.get(0);

            // Only the *other* replica goes down - the primary stays up and reachable, so it will run the
            // processor successfully. Only replicating the resulting delta at CL.ALL (both replicas required)
            // will fail, since only the primary itself remains alive among the two natural replicas.
            backup.shutdown().get();

            try
            {
                // Dispatched from the outsider, which is neither the primary nor a replica - this must cross the
                // wire (Verb.ENTRYPROCESSOR_REQ) to the primary, which runs IncrementProcessor, then fails to
                // replicate at CL.ALL, and reports WRITE_FAILURE back over Verb.ENTRYPROCESSOR_RSP.
                invokeIncrement(outsider, key, ConsistencyLevel.ALL);
                fail("expected replication to fail at CL.ALL with one of two natural replicas down");
            }
            catch (Throwable t)
            {
                String message = t.getMessage() == null ? "" : t.getMessage();
                assertTrue("expected the failure to be reported as WRITE_FAILURE, was: " + t,
                           message.contains("WRITE_FAILURE"));
            }
        }
    }
}
