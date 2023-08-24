/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.scheduler.mapreduce.MRTableCommitMetadataCache;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TableCommitMetadataOperator
        implements SourceOperator, Closeable
{
    public static class TableCommitMetadataOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final MRTableCommitMetadataCache tableCommitMetadataCache;
        private final PagesSerdeFactory serdeFactory;
        private ExchangeClient exchangeClient;
        private boolean closed;

        public TableCommitMetadataOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                MRTableCommitMetadataCache tableCommitMetadataCache,
                PagesSerdeFactory serdeFactory)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.tableCommitMetadataCache = requireNonNull(tableCommitMetadataCache, "taskExchangeClientManager is null");
            this.serdeFactory = serdeFactory;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, TableCommitMetadataOperator.class.getSimpleName());

            return new TableCommitMetadataOperator(
                    operatorContext,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    tableCommitMetadataCache);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final MRTableCommitMetadataCache tableCommitMetadataCache;
    private final PagesSerde serde;
    private ListenableFuture<?> isBlocked = NOT_BLOCKED;

    private Iterator<SerializedPage> serializedPageList;

    public TableCommitMetadataOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PagesSerde serde,
            MRTableCommitMetadataCache tableCommitMetadataCache)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.tableCommitMetadataCache = requireNonNull(tableCommitMetadataCache, "exchangeClient is null");
        this.serde = requireNonNull(serde, "serde is null");
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit scheduledSplit)
    {
        Split split = requireNonNull(scheduledSplit, "scheduledSplit is null").getSplit();
        requireNonNull(split, "split is null");
        serializedPageList = tableCommitMetadataCache.getTableCommitMetadata().iterator();

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return !serializedPageList.hasNext();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (!serializedPageList.hasNext()) {
            return null;
        }

        SerializedPage page = serializedPageList.next();
        operatorContext.recordRawInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
//        exchangeClient.close();
    }
}
