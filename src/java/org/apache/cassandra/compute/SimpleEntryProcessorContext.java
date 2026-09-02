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
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Default {@link EntryProcessorContext}, backed directly by Cassandra's own {@link Mutation.SimpleBuilder} /
 * {@link PartitionUpdate.SimpleBuilder} / {@link Row.SimpleBuilder} chain. {@link #delta()} exposes the row
 * builder as-is, so every column a processor adds becomes exactly one cell in the resulting {@link Mutation} —
 * nothing here diffs or snapshots the row; the processor's own calls to {@code delta().add(...)} are the entirety
 * of the change.
 * <p>
 * The map abstraction this supports has no clustering columns: a "map entry" is one partition's static row, so
 * the row builder is obtained via {@code PartitionUpdate.SimpleBuilder#row()} with no clustering values.
 */
public final class SimpleEntryProcessorContext implements EntryProcessorContext
{
    private final TableMetadata table;
    private final DecoratedKey key;
    private final Row currentRow;
    private final Mutation.SimpleBuilder mutationBuilder;
    private final Row.SimpleBuilder rowBuilder;

    public SimpleEntryProcessorContext(String keyspaceName, TableMetadata table, DecoratedKey key, Row currentRow)
    {
        this.table = table;
        this.key = key;
        this.currentRow = currentRow;
        this.mutationBuilder = Mutation.simpleBuilder(keyspaceName, key);
        PartitionUpdate.SimpleBuilder partitionBuilder = mutationBuilder.update(table);
        this.rowBuilder = partitionBuilder.row();
    }

    @Override
    public DecoratedKey key()
    {
        return key;
    }

    @Override
    public TableMetadata table()
    {
        return table;
    }

    @Override
    public Row currentRow()
    {
        return currentRow;
    }

    @Override
    public Row.SimpleBuilder delta()
    {
        return rowBuilder;
    }

    /**
     * Builds the {@link Mutation} representing everything the processor wrote via {@link #delta()}. This is the
     * value handed to Cassandra's normal write path ({@code StorageProxy.mutate}) — see the technical spec's
     * "BackupEntryProcessor is not user-authored logic" decision.
     */
    public Mutation buildMutation()
    {
        return mutationBuilder.build();
    }
}
