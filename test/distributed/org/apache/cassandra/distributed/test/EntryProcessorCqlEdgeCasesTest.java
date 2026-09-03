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
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.compute.ComputeQueryHandler;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins down the three v1 scope limits documented on {@code EntryProcessorCallStatement}'s class javadoc, none of
 * which had a test proving the documented behavior actually happens: {@code PREPARE CALL ENTRYPROCESSOR(...)}
 * fails with a normal CQL parse error; a malformed/near-miss {@code CALL ENTRYPROCESSOR(...)} falls through to
 * normal CQL parsing rather than being swallowed or mishandled by the compute layer; and only {@code text}/
 * {@code varchar} partition keys are actually supported, despite nothing enforcing that at the CQL entry point.
 * <p>
 * Drives {@code ClientState.getCQLQueryHandler()} directly, same as {@code EntryProcessorDistributedTest}, since
 * {@code coordinator().execute()}/{@code executeInternal()} bypass the query-handler swap point in this dtest-api
 * version.
 */
public class EntryProcessorCqlEdgeCasesTest extends TestBaseImpl
{
    @BeforeClass
    public static void setCustomQueryHandler()
    {
        System.setProperty("cassandra.custom_query_handler_class", ComputeQueryHandler.class.getName());
    }

    @Test
    public void prepareFailsWithANormalCqlParseErrorRatherThanSomethingConfusing() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            IInvokableInstance instance = cluster.get(1);
            String prepareQuery = String.format("CALL ENTRYPROCESSOR('%s', 'widgets', 'k', '%s')",
                                                 KEYSPACE, EntryProcessorDistributedTest.IncrementProcessor.class.getName());

            String outcome = instance.callOnInstance((SerializableCallable<String>) () -> {
                QueryHandler handler = ClientState.getCQLQueryHandler();
                ClientState clientState = ClientState.forInternalCalls();
                try
                {
                    handler.prepare(prepareQuery, clientState, Collections.emptyMap());
                    return "no exception thrown";
                }
                catch (RequestValidationException e)
                {
                    // ComputeQueryHandler.prepare() delegates straight to QueryProcessor.instance.prepare(...),
                    // which parses via the ANTLR grammar - "CALL" isn't a grammar keyword, so this is expected to
                    // fail the same way any other unrecognized statement would, not with anything specific to the
                    // compute layer.
                    return e.getClass().getName() + ": " + e.getMessage();
                }
                catch (RuntimeException e)
                {
                    return "UNEXPECTED " + e.getClass().getName() + ": " + e.getMessage();
                }
            });

            assertTrue("PREPARE should fail with a normal CQL validation/syntax exception, not something else or "
                       + "nothing at all - got: " + outcome,
                       outcome.startsWith("org.apache.cassandra.exceptions."));
            assertTrue("expected an UNEXPECTED-free, ordinary CQL exception type, got: " + outcome,
                       !outcome.startsWith("UNEXPECTED"));
        }
    }

    @Test
    public void malformedCallFallsThroughToNormalCqlParsingRatherThanBeingSwallowed() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.widgets (id text PRIMARY KEY, val int)"));

            IInvokableInstance instance = cluster.get(1);
            // One argument short of the required 4 (keyspace, table, key, processorClassName) - PATTERN can't
            // match this, so EntryProcessorCallStatement.tryParse() must return null and let it fall through.
            String malformed = String.format("CALL ENTRYPROCESSOR('%s', 'widgets', 'k')", KEYSPACE);

            String outcome = instance.callOnInstance((SerializableCallable<String>) () -> {
                QueryHandler handler = ClientState.getCQLQueryHandler();
                QueryState queryState = QueryState.forInternalCalls();
                QueryOptions options = QueryOptions.DEFAULT;
                try
                {
                    handler.parse(malformed, queryState, options);
                    return "no exception thrown";
                }
                catch (RuntimeException e)
                {
                    // Any ordinary CQL parse failure is fine here - the point is only that it's a normal CQL
                    // error (from QueryProcessor's own ANTLR-based parse()), not an exception or behavior that's
                    // specific to (or mangled by) the compute layer's own parsing attempt.
                    return e.getClass().getName() + ": " + e.getMessage();
                }
            });

            assertTrue("expected this to fail via normal CQL parsing (a RuntimeException from QueryProcessor's "
                       + "grammar), got: " + outcome,
                       outcome.contains("Exception"));
        }
    }

    /**
     * {@code EntryProcessorCallStatement} always decomposes the {@code key} argument via {@code UTF8Type},
     * regardless of the target table's actual partition key type - documented as a deliberate v1 scope limit, but
     * nothing enforces it. Against a table with an {@code int} partition key, {@code UTF8Type.decompose("123")}
     * (3 bytes: the ASCII digits) does not produce the same bytes as {@code Int32Type.decompose(123)} (4 bytes:
     * the big-endian int) - so this resolves to, reads from, and writes to a *different, spurious* partition than
     * the one holding the real {@code id = 123} row, rather than raising any kind of "wrong key type" error.
     * <p>
     * This test pins down exactly what that produces: the real row is untouched (confirmed by reading it back),
     * but a full-partition scan of the table is checked afterward for whether the spurious partition's malformed
     * key bytes cause a decode failure - see the assertions/comments below for what was actually observed.
     */
    @Test
    public void nonTextPartitionKeyResolvesToASpuriousPartitionNotTheRealRow() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.int_widgets (id int PRIMARY KEY, val int)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.int_widgets (id, val) VALUES (123, 42)"),
                                            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);

            IInvokableInstance instance = cluster.get(1);
            String call = String.format("CALL ENTRYPROCESSOR('%s', 'int_widgets', '123', '%s')",
                                         KEYSPACE, EntryProcessorDistributedTest.IncrementProcessor.class.getName());

            int result = instance.callOnInstance((SerializableCallable<Integer>) () -> {
                QueryHandler handler = ClientState.getCQLQueryHandler();
                QueryState queryState = QueryState.forInternalCalls();
                QueryOptions options = QueryOptions.DEFAULT;
                CQLStatement statement = handler.parse(call, queryState, options);
                ResultMessage message = handler.process(statement, queryState, options, Collections.emptyMap(),
                                                          Dispatcher.RequestTime.forImmediateExecution());
                ResultMessage.Rows rows = (ResultMessage.Rows) message;
                ByteBuffer bytes = rows.result.rows.get(0).get(0);
                return Int32Type.instance.compose(bytes);
            });

            // No error at all - the call "succeeds", against a partition that has nothing to do with id=123. Its
            // currentRow() read finds nothing there (no data was ever written under UTF8-encoded "123" bytes), so
            // IncrementProcessor treats it as a fresh entry and returns 1, not the "should be 43" a caller might
            // reasonably expect if they assumed the int key was handled correctly.
            assertEquals("the processor ran against an empty/spurious partition, not the real int-keyed row",
                         1, result);

            Object[][] realRow = instance.executeInternal(withKeyspace("SELECT val FROM %s.int_widgets WHERE id = ?"), 123);
            assertEquals("the real id=123 row must be untouched by a CALL that resolved to a different partition",
                         1, realRow.length);
            assertEquals(42, realRow[0][0]);

            // Does the spurious partition's malformed key bytes break a full-table scan afterward? This is the
            // "how bad is this, actually" check - see this test's javadoc. Run via executeInternal directly (not
            // callOnInstance) so the failure surfaces here without needing to round-trip it back first.
            try
            {
                Object[][] scanned = instance.executeInternal(withKeyspace("SELECT id, val FROM %s.int_widgets"));
                fail("expected a full-table scan to fail decoding the spurious partition's malformed key bytes as "
                     + "an int, but it returned " + scanned.length + " rows with no error - this is a MORE benign "
                     + "outcome than expected, worth re-confirming this comment still matches reality if this test "
                     + "starts failing here");
            }
            catch (Throwable t)
            {
                // Observed empirically (not "expected" in the sense of a graceful, well-typed error): a raw
                // java.lang.IndexOutOfBoundsException, not a clean MarshalException. Int32Type's decompose reads a
                // fixed 4 bytes; UTF8Type.decompose("123") produced only 3 (the ASCII digits), so decoding the
                // spurious partition's key as an int during the scan is an unchecked buffer-underflow read, not a
                // validated type-mismatch check. This is worse than "wrong but harmless": a single CALL against
                // the wrong key type leaves behind a partition that breaks a plain, ordinary SELECT * over the
                // *entire* table with an unhandled low-level exception, not a clear error naming what's wrong.
                assertTrue("expected an IndexOutOfBoundsException from the undersized malformed key bytes, got: " + t,
                           t instanceof IndexOutOfBoundsException);
            }
        }
    }
}
