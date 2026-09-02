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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/**
 * The client-facing entry point decided in the technical spec: swapped in for the whole process via the
 * {@code cassandra.custom_query_handler_class} system property (wired in {@code service/ClientState.java}), set to
 * {@code org.apache.cassandra.compute.ComputeQueryHandler} at JVM startup — not something this project's code sets
 * itself, since it's a single JVM-wide swap point Cassandra reads once at class-init.
 * <p>
 * Only {@link #parse} is actually overridden: it recognizes the {@code CALL ENTRYPROCESSOR(...)} syntax (see
 * {@link EntryProcessorCallStatement}) and returns our own {@link CQLStatement} implementation for it, falling
 * back to {@link QueryProcessor#instance} for everything else. Every other method delegates to
 * {@link QueryProcessor#instance} unconditionally and unmodified — {@code process()} in particular works generically
 * against any {@link CQLStatement} (it just calls {@code authorize}/{@code validate}/{@code execute} on whatever it's
 * given), so it needs no special-casing here at all. {@code prepare}/{@code processPrepared}/{@code processBatch}
 * are not extended to understand our custom syntax in this version — preparing a {@code CALL ENTRYPROCESSOR(...)}
 * statement will fail with a normal CQL parse error; callers use simple (unprepared) execution for it.
 */
public final class ComputeQueryHandler implements QueryHandler
{
    public ComputeQueryHandler()
    {
    }

    @Override
    public CQLStatement parse(String queryString, QueryState queryState, QueryOptions options)
    {
        EntryProcessorCallStatement statement = EntryProcessorCallStatement.tryParse(queryString);
        if (statement != null)
            return statement;

        return QueryProcessor.instance.parse(queryString, queryState, options);
    }

    @Override
    public ResultMessage process(CQLStatement statement,
                                  QueryState state,
                                  QueryOptions options,
                                  Map<String, ByteBuffer> customPayload,
                                  Dispatcher.RequestTime requestTime) throws RequestExecutionException, RequestValidationException
    {
        return QueryProcessor.instance.process(statement, state, options, customPayload, requestTime);
    }

    @Override
    public ResultMessage.Prepared prepare(String query, ClientState clientState, Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return QueryProcessor.instance.prepare(query, clientState, customPayload);
    }

    @Override
    public QueryHandler.Prepared getPrepared(MD5Digest id)
    {
        return QueryProcessor.instance.getPrepared(id);
    }

    @Override
    public ResultMessage processPrepared(CQLStatement statement,
                                          QueryState state,
                                          QueryOptions options,
                                          Map<String, ByteBuffer> customPayload,
                                          Dispatcher.RequestTime requestTime) throws RequestExecutionException, RequestValidationException
    {
        return QueryProcessor.instance.processPrepared(statement, state, options, customPayload, requestTime);
    }

    @Override
    public ResultMessage processBatch(BatchStatement statement,
                                       QueryState state,
                                       BatchQueryOptions options,
                                       Map<String, ByteBuffer> customPayload,
                                       Dispatcher.RequestTime requestTime) throws RequestExecutionException, RequestValidationException
    {
        return QueryProcessor.instance.processBatch(statement, state, options, customPayload, requestTime);
    }
}
