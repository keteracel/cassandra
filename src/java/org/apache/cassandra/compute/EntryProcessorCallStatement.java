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
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The {@link CQLStatement} behind {@code CALL ENTRYPROCESSOR('keyspace', 'table', 'key', 'processorClassName'[,
 * 'consistencyLevel'])} — the client-facing entry point decided in the technical spec: a purpose-built call syntax
 * intercepted by {@link ComputeQueryHandler}, rather than a new protocol/port, so callers keep using a normal CQL
 * driver connection.
 * <p>
 * This is hand-parsed via {@link #PATTERN} rather than added to Cassandra's ANTLR grammar — the grammar is
 * generated, shared by every statement type, and modifying it is a much larger and riskier undertaking than this
 * project's other changes; a regex over a small, fixed-arity call form is deliberately simple instead. All
 * arguments are plain single-quoted string literals using CQL's own escaping convention ({@code ''} for a literal
 * quote); there is no support for other literal forms (numbers, bind markers, nested expressions) in this version.
 * <p>
 * <b>v1 scope limits, deliberate</b>: only {@code text}/{@code varchar} partition keys are supported (the key
 * argument is always decomposed via {@link UTF8Type}); the result is always returned as a single {@code blob}
 * column, since this statement has no way to know the processor's actual result type; and {@link #authorize} is a
 * no-op — there is no permission model yet, per the product spec's decision to leave security out of scope.
 */
public final class EntryProcessorCallStatement implements CQLStatement
{
    private static final Pattern PATTERN = Pattern.compile(
        "(?is)^\\s*CALL\\s+ENTRYPROCESSOR\\s*\\(\\s*"
        + "'((?:[^']|'')*)'\\s*,\\s*"                   // keyspace
        + "'((?:[^']|'')*)'\\s*,\\s*"                   // table
        + "'((?:[^']|'')*)'\\s*,\\s*"                   // key
        + "'((?:[^']|'')*)'\\s*"                        // processor class name
        + "(?:,\\s*'((?:[^']|'')*)'\\s*)?"               // optional consistency level
        + "\\)\\s*;?\\s*$");

    private final String keyspace;
    private final String table;
    private final String key;
    private final String processorClassName;
    private final ConsistencyLevel consistencyLevel;

    private EntryProcessorCallStatement(String keyspace, String table, String key, String processorClassName, ConsistencyLevel consistencyLevel)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.key = key;
        this.processorClassName = processorClassName;
        this.consistencyLevel = consistencyLevel;
    }

    /**
     * @return a parsed statement if {@code queryString} matches the {@code CALL ENTRYPROCESSOR(...)} syntax, or
     * {@code null} if it doesn't (in which case the caller should fall back to normal CQL parsing).
     */
    static EntryProcessorCallStatement tryParse(String queryString)
    {
        Matcher m = PATTERN.matcher(queryString);
        if (!m.matches())
            return null;

        String keyspace = unescape(m.group(1));
        String table = unescape(m.group(2));
        String key = unescape(m.group(3));
        String processorClassName = unescape(m.group(4));
        String clText = m.group(5);
        ConsistencyLevel consistencyLevel = clText == null ? ConsistencyLevel.ONE : ConsistencyLevel.valueOf(unescape(clText).toUpperCase());

        return new EntryProcessorCallStatement(keyspace, table, key, processorClassName, consistencyLevel);
    }

    private static String unescape(String s)
    {
        return s.replace("''", "'");
    }

    @Override
    public void authorize(ClientState state)
    {
        // No permission model yet - see the product spec's out-of-scope decision on security/authn/authz.
    }

    @Override
    public void validate(ClientState state)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        if (metadata == null)
            throw new InvalidRequestException("Unknown table: " + keyspace + "." + table);

        checkTextPartitionKey(metadata);
    }

    /**
     * Enforces the v1 scope limit documented on this class: the key argument is always decomposed via
     * {@link UTF8Type}, so a table whose partition key isn't {@code text}/{@code varchar}/{@code ascii} would
     * otherwise get a spurious partition written with the wrong byte length for its key type — which doesn't just
     * fail this call, it corrupts later reads of the *whole table* for every other caller (a raw
     * {@code IndexOutOfBoundsException} decoding that partition's key, not a clean, catchable error). Reject the
     * call outright instead, since a wrong-shaped table is knowable up front and never becomes valid mid-call - no
     * need to repeat this check in {@link #doExecute()}, unlike the table-existence check above (which can
     * legitimately change between validate() and execute() via a concurrent DROP TABLE).
     */
    private void checkTextPartitionKey(TableMetadata metadata)
    {
        AbstractType<?> keyType = metadata.partitionKeyType;
        if (!(keyType instanceof UTF8Type) && !(keyType instanceof AsciiType))
            throw new InvalidRequestException(
                "CALL ENTRYPROCESSOR only supports text/varchar/ascii partition keys, but "
                + keyspace + "." + table + " has partition key type " + keyType.asCQL3Type());
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        return doExecute();
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return doExecute();
    }

    private ResultMessage doExecute()
    {
        Keyspace ks = Keyspace.open(keyspace);
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        if (metadata == null)
            throw new InvalidRequestException("Unknown table: " + keyspace + "." + table);

        DecoratedKey dk = metadata.partitioner.decorateKey(UTF8Type.instance.decompose(key));
        EntryProcessorRequest request = new EntryProcessorRequest(metadata.id,
                                                                    dk.getKey(),
                                                                    processorClassName,
                                                                    ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                    consistencyLevel);

        EntryProcessorResponse response;
        try
        {
            response = EntryDispatch.dispatch(ks, dk, request).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException("EntryProcessor dispatch failed: " + e, e);
        }

        switch (response.status)
        {
            case PROCESSOR_FAILURE:
                throw new InvalidRequestException("EntryProcessor failed: " + response.failureMessage);
            case WRITE_FAILURE:
                throw new InvalidRequestException("Write failed: " + response.failureMessage);
            case SUCCESS:
                return successResult(response.result);
            default:
                throw new AssertionError("Unknown EntryProcessorResponse status: " + response.status);
        }
    }

    private ResultMessage successResult(ByteBuffer result)
    {
        ColumnSpecification spec = new ColumnSpecification(keyspace, table, new ColumnIdentifier("result", false), BytesType.instance);
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Collections.singletonList(spec));
        ResultSet resultSet = new ResultSet(resultMetadata, Collections.singletonList(Collections.singletonList(result)));
        return new ResultMessage.Rows(resultSet);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.UPDATE, keyspace, table);
    }
}
