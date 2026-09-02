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

import java.util.concurrent.locks.Lock;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;

/**
 * Per-key mutual exclusion for {@link EntryProcessor} execution.
 * <p>
 * This mirrors Cassandra's own precedent for exactly this problem: {@code CounterMutation}'s striped cell-level
 * locks, which serialize a local read-modify-write against concurrent updates to the same partition, since a
 * counter increment must read the current value before writing the new one. An {@link EntryProcessor} invocation
 * is the same shape of operation (read {@link EntryProcessorContext#currentRow()}, compute a delta, hand it to the
 * write path) and needs the same treatment: two concurrent invocations for the same key must not interleave.
 * <p>
 * <b>Scope</b>: this locks out concurrent invocations that go through the compute layer — {@link EntryProcessor}
 * execution today, and any future map put/remove operations built on the same path (see the technical spec's
 * client-facing entry point step). It does not, and architecturally cannot on its own, block a client issuing a
 * plain CQL write directly against the backing table outside the map API — that would require hooking Cassandra's
 * universal write path for every table, not just the ones this compute layer manages, which is a materially
 * bigger and riskier change than anything else in this project and is deliberately not done here.
 */
final class EntryLocks
{
    private EntryLocks()
    {
    }

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    static Lock lockFor(TableId tableId, DecoratedKey key)
    {
        return LOCKS.get(Objects.hashCode(tableId, key));
    }
}
