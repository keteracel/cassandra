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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Resolves which node owns a key for the purposes of {@link EntryProcessor} dispatch.
 * <p>
 * "Primary" here means the first entry in Cassandra's own natural-replica ordering for the key
 * ({@link AbstractReplicationStrategy#getNaturalReplicasForToken}) — this is a project convention, not a
 * Cassandra-enforced invariant. Inside Cassandra itself, {@code replicas.get(0)} is only special-cased for
 * repair-range bookkeeping (see {@code StorageService#getPrimaryRangesForEndpoint}), not for write coordination;
 * Cassandra's own write path treats all natural replicas equally. Callers must always re-resolve through this
 * class rather than caching a result, since the ordering can change when cluster topology changes.
 */
public final class EntryOwnership
{
    private EntryOwnership()
    {
    }

    public static EndpointsForToken naturalReplicas(Keyspace keyspace, DecoratedKey key)
    {
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        return strategy.getNaturalReplicasForToken(key);
    }

    public static Replica primary(Keyspace keyspace, DecoratedKey key)
    {
        return naturalReplicas(keyspace, key).get(0);
    }

    public static boolean isLocalPrimary(Keyspace keyspace, DecoratedKey key)
    {
        return primary(keyspace, key).endpoint().equals(FBUtilities.getBroadcastAddressAndPort());
    }
}
