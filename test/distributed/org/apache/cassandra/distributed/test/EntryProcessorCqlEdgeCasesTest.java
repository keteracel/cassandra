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

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.compute.ComputeQueryHandler;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins down the three v1 scope limits documented on {@code EntryProcessorCallStatement}'s class javadoc, none of
 * which had a test proving the documented behavior actually happens: {@code PREPARE CALL ENTRYPROCESSOR(...)}
 * fails with a normal CQL parse error; a malformed/near-miss {@code CALL ENTRYPROCESSOR(...)} falls through to
 * normal CQL parsing rather than being swallowed or mishandled by the compute layer; and only {@code text}/
 * {@code varchar}/{@code ascii} partition keys are supported - the third case found a real bug (GitHub issue #12:
 * a rejected-nothing call silently corrupted later reads of the whole table) rather than just an untested claim,
 * fixed in {@code EntryProcessorCallStatement.validate()} and re-verified here against the fix.
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
     * {@code EntryProcessorCallStatement} used to always decompose the {@code key} argument via {@code UTF8Type}
     * regardless of the target table's actual partition key type - against an {@code int}-keyed table,
     * {@code UTF8Type.decompose("123")} (3 bytes: the ASCII digits) doesn't produce the same bytes as
     * {@code Int32Type.decompose(123)} (4 bytes: the big-endian int), so the call silently wrote a *spurious*
     * partition with the wrong byte length for its key type. That didn't just resolve to the wrong row - a later,
     * completely unrelated {@code SELECT *} over the whole table then threw a raw
     * {@code java.lang.IndexOutOfBoundsException} trying to decode that partition's malformed key, breaking
     * ordinary reads for every other caller (GitHub issue #12).
     * <p>
     * {@code EntryProcessorCallStatement.validate()} now checks the target table's partition key type up front and
     * rejects anything other than text/varchar/ascii with a clear {@link InvalidRequestException}, before ever
     * touching storage. This test proves the fix: the call fails cleanly, the real row is untouched, and - unlike
     * before the fix - a full-table scan afterward is completely unaffected, since no spurious partition was ever
     * written.
     */
    @Test
    public void nonTextPartitionKeyIsRejectedCleanlyRatherThanCorruptingTheTable() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.int_widgets (id int PRIMARY KEY, val int)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.int_widgets (id, val) VALUES (123, 42)"),
                                            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);

            IInvokableInstance instance = cluster.get(1);
            String call = String.format("CALL ENTRYPROCESSOR('%s', 'int_widgets', '123', '%s')",
                                         KEYSPACE, EntryProcessorDistributedTest.IncrementProcessor.class.getName());

            String outcome = instance.callOnInstance((SerializableCallable<String>) () -> {
                QueryHandler handler = ClientState.getCQLQueryHandler();
                QueryState queryState = QueryState.forInternalCalls();
                QueryOptions options = QueryOptions.DEFAULT;
                try
                {
                    CQLStatement statement = handler.parse(call, queryState, options);
                    handler.process(statement, queryState, options, Collections.emptyMap(),
                                     Dispatcher.RequestTime.forImmediateExecution());
                    return "no exception thrown";
                }
                catch (RequestValidationException e)
                {
                    return e.getClass().getName() + ": " + e.getMessage();
                }
            });

            assertTrue("expected a clean InvalidRequestException naming the partition key type mismatch, got: " + outcome,
                       outcome.contains("InvalidRequestException") && outcome.contains("partition key"));

            Object[][] realRow = instance.executeInternal(withKeyspace("SELECT val FROM %s.int_widgets WHERE id = ?"), 123);
            assertEquals("the real id=123 row must be untouched by a rejected call",
                         1, realRow.length);
            assertEquals(42, realRow[0][0]);

            // The fix's whole point: no spurious partition was ever written, so an ordinary full-table scan is
            // completely unaffected - this used to throw a raw IndexOutOfBoundsException before the fix.
            Object[][] scanned = instance.executeInternal(withKeyspace("SELECT id, val FROM %s.int_widgets"));
            assertEquals("only the real row should exist - no spurious partition left behind",
                         1, scanned.length);
        }
    }
}
