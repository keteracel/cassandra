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

/**
 * Proves {@code EntryLocks}'s lock-timeout path actually fires and reports {@code WRITE_FAILURE} - before this
 * test, {@code EntryProcessorDistributedTest.concurrentInvocationsForSameKeyDoNotLoseUpdates} proved contending
 * invocations serialize correctly and none are lost, but its 25 fast increments at {@code ConsistencyLevel.ONE}
 * never came close to {@code write_request_timeout} (2000ms default) - so the "wait and eventually succeed" side
 * of {@code EntryLocks} was covered, but not the "wait, give up, and fail clearly" side
 * ({@code EntryProcessorRequestHandler.execute()}'s {@code !lock.tryLock(...)} branch,
 * {@code "Timed out waiting for exclusive access to key "}).
 * <p>
 * {@code write_request_timeout} is lowered to 1000ms here (via {@code config.set(...)}, the pattern several other
 * dtests in this suite already use, e.g. {@code HintsDisabledTest}) purely so the test runs fast; the mechanism
 * under test doesn't depend on the specific value.
 */
public class EntryLocksContentionTest extends TestBaseImpl
{
    /**
     * Sleeps well past {@code write_request_timeout} while holding {@code EntryLocks}' per-key lock (acquired by
     * {@link org.apache.cassandra.compute.EntryProcessorRequestHandler#execute} before {@code process()} is ever
     * called) - long enough that a second, concurrent invocation for the same key is guaranteed to still be
     * waiting on the lock when its own timeout expires.
     */
    public static class SlowProcessor implements EntryProcessor<ByteBuffer>
    {
        public static final long SLEEP_MILLIS = 4000;

        public SlowProcessor()
        {
        }

        @Override
        public ByteBuffer process(EntryProcessorContext ctx) throws Exception
        {
            Thread.sleep(SLEEP_MILLIS);
            ctx.delta().add("val", 1);
            return Int32Type.instance.decompose(1);
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

    /**
     * Returns {@code "<Status>|<failureMessage>"} rather than the {@link EntryProcessorResponse} itself - that
     * type isn't {@code Serializable} and is loaded by each dtest instance's own isolated classloader, so it can't
     * safely cross back to the test driver via {@code callOnInstance}. This still lets the test assert on the
     * exact status and message, not just pattern-match a wrapping exception's text.
     */
    private static String invokeAndDescribeStatus(IInvokableInstance instance, String key, ConsistencyLevel cl)
    {
        return instance.callOnInstance((SerializableCallable<String>) () -> {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "widgets");
            DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(key));
            EntryProcessorRequest request = new EntryProcessorRequest(table.id,
                                                                        dk.getKey(),
                                                                        SlowProcessor.class.getName(),
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
            return response.status.name() + "|" + response.failureMessage;
        });
    }

    @Test
    public void secondInvocationTimesOutWaitingForTheLockAndReportsWriteFailure() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                            .withConfig(config -> config.with(GOSSIP, NETWORK)
                                                                         .set("write_request_timeout", "1000ms"))
                                            .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            String key = "lock-contention-key";
            IInvokableInstance primary = null;
            for (int i = 1; i <= 3; i++)
                if (isPrimary(cluster.get(i), key))
                    primary = cluster.get(i);
            assertTrue("test setup: expected exactly one primary", primary != null);
            IInvokableInstance thePrimary = primary;

            ExecutorService pool = Executors.newSingleThreadExecutor();
            try
            {
                // Kicks off the slow invocation, which will hold EntryLocks' lock for this key for
                // SlowProcessor.SLEEP_MILLIS (4000ms) - run asynchronously since we need the test thread free to
                // dispatch the second, contending invocation while this one is still in flight.
                Future<String> slowInvocation = pool.submit(() -> invokeAndDescribeStatus(thePrimary, key, ConsistencyLevel.ONE));

                // Give the slow invocation a generous head start to actually acquire the lock and enter its sleep
                // before the second invocation starts competing for it - this only needs to be shorter than
                // SlowProcessor.SLEEP_MILLIS minus write_request_timeout (4000 - 1000 = 3000ms of margin), so
                // 500ms is comfortable without making the test needlessly slow.
                Thread.sleep(500);

                long startNanos = System.nanoTime();
                String secondResult = invokeAndDescribeStatus(thePrimary, key, ConsistencyLevel.ONE);
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

                assertTrue("expected the second invocation to report WRITE_FAILURE (was: " + secondResult + ")",
                           secondResult.startsWith("WRITE_FAILURE|"));
                assertTrue("expected the lock-timeout message, got: " + secondResult,
                           secondResult.contains("Timed out waiting for exclusive access"));
                // Should time out at roughly write_request_timeout (1000ms), not hang for anywhere near
                // SlowProcessor.SLEEP_MILLIS (4000ms) - a generous upper bound keeps this robust to scheduling
                // jitter while still clearly distinguishing "timed out promptly" from "waited for the whole lock".
                assertTrue("expected the second invocation to time out well before the slow one finishes, took " + elapsedMs + "ms",
                           elapsedMs < 3000);

                assertEquals("the slow invocation itself should still have succeeded once it finished",
                             "SUCCESS|null", slowInvocation.get());
            }
            finally
            {
                pool.shutdown();
            }
        }
    }
}
