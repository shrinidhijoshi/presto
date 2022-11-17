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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.facebook.presto.operator.WorkProcessor.TransformationState.finished;
import static com.facebook.presto.operator.WorkProcessor.TransformationState.needsMoreData;
import static com.facebook.presto.operator.WorkProcessor.TransformationState.ofResult;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class SpillableGroupedTopNBuilder
        implements GroupedTopNBuilder
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SpillableGroupedTopNBuilder.class).instanceSize();

    private final Supplier<InMemoryGroupedTopNBuilder> inputInMemoryGroupedTopNBuilderSupplier;
    private final Supplier<InMemoryGroupedTopNBuilder> outputInMemoryGroupedTopNBuilderSupplier;
    private final SpillerFactory spillerFactory;
    private final List<Type> sourceTypes;
    private final List<Type> partitionTypes;
    private final List<Integer> partitionChannels;

    private InMemoryGroupedTopNBuilder inputInMemoryGroupedTopNBuilder;
    private InMemoryGroupedTopNBuilder outputInMemoryGroupedTopNBuilder;

    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final AggregatedMemoryContext aggregatedMemoryContextForMerge;
    private final AggregatedMemoryContext aggregatedMemoryContextForSpill;
    private final DriverYieldSignal driverYieldSignal;
    private final SpillContext spillContext;

    private final long unspillMemoryLimit;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);

    public SpillableGroupedTopNBuilder(
            List<Type> sourceTypes,
            List<Type> partitionTypes,
            List<Integer> partitionChannels,
            Supplier<InMemoryGroupedTopNBuilder> inputInMemoryGroupedTopNBuilderSupplier,
            Supplier<InMemoryGroupedTopNBuilder> outputInMemoryGroupedTopNBuilderSupplier,
            long unspillMemoryLimit,
            LocalMemoryContext localUserMemoryContext,
            LocalMemoryContext localRevocableMemoryContext,
            AggregatedMemoryContext aggregatedMemoryContextForMerge,
            AggregatedMemoryContext aggregatedMemoryContextForSpill,
            SpillContext spillContext,
            DriverYieldSignal driverYieldSignal,
            SpillerFactory spillerFactory)
    {
        this.inputInMemoryGroupedTopNBuilderSupplier = inputInMemoryGroupedTopNBuilderSupplier;
        this.outputInMemoryGroupedTopNBuilderSupplier = outputInMemoryGroupedTopNBuilderSupplier;
        this.spillerFactory = spillerFactory;
        this.sourceTypes = sourceTypes;
        this.partitionTypes = partitionTypes;
        this.partitionChannels = partitionChannels;

        initializeInputInMemoryGroupedTopNBuilder();

        this.localUserMemoryContext = localUserMemoryContext;
        this.localRevocableMemoryContext = localRevocableMemoryContext;
        this.aggregatedMemoryContextForMerge = aggregatedMemoryContextForMerge;
        this.aggregatedMemoryContextForSpill = aggregatedMemoryContextForSpill;
        this.driverYieldSignal = driverYieldSignal;
        this.spillContext = spillContext;

        this.unspillMemoryLimit = unspillMemoryLimit;
    }

    public Work<?> processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        Work<?> result = inputInMemoryGroupedTopNBuilder.processPage(page);
        return result;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (spillInProgress.isDone()) {
            // check for exception from previous spill for early failure
            checkSpillSucceeded(spillInProgress);
            return true;
        }
        return false;
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        // spill could be in progress.
        checkSpillSucceeded(spillInProgress);

        // Convert revocable memory to user memory as returned Iterator holds on to memory so we no longer can revoke.
        if (!inputInMemoryGroupedTopNBuilder.isEmpty()) {
            long currentRevocableBytes = localRevocableMemoryContext.getBytes();
            localRevocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                localRevocableMemoryContext.setBytes(currentRevocableBytes);
                checkSpillSucceeded(spillToDisk());
            }
        }

        if (!spiller.isPresent()) {
            return inputInMemoryGroupedTopNBuilder.buildResult();
        }

        // Spill the remaining collected input
        // TODO: Possible Optimization here is to not spill the last remaining buffered input
        // and instead do a memory+disk sort merge. SpillableHashAggregationBuilder does this
        if (!inputInMemoryGroupedTopNBuilder.isEmpty()) {
            checkSpillSucceeded(spillToDisk());
            verify(inputInMemoryGroupedTopNBuilder.isEmpty());
        }

        // Collect all spill streams to merge-sort
        List<WorkProcessor<Page>> sortedPageStreams = ImmutableList.<WorkProcessor<Page>>builder()
                .addAll(spiller.get().getSpills().stream()
                        .map(WorkProcessor::fromIterator)
                        .collect(toImmutableList()))
                .build();

        // Sort-Merge the rows and produce group-by-group output
        return getFinalResult(sortedPageStreams);
    }

    @Override
    public GroupByHash getGroupByHash()
    {
        return inputInMemoryGroupedTopNBuilder.getGroupByHash();
    }

    @Override
    public boolean isEmpty()
    {
        return inputInMemoryGroupedTopNBuilder.isEmpty() && outputInMemoryGroupedTopNBuilder.isEmpty();
    }

    @Override
    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE + inputInMemoryGroupedTopNBuilder.getEstimatedSizeInBytes();
    }

    @Override
    public ListenableFuture<?> updateMemoryReservations()
    {
        ListenableFuture<?> inputBuilderFuture = inputInMemoryGroupedTopNBuilder.updateMemoryReservations();

        ListenableFuture<?> outputBuilderFuture = null;
        if (outputInMemoryGroupedTopNBuilder != null) {
            outputBuilderFuture = outputInMemoryGroupedTopNBuilder.updateMemoryReservations();
        }

        if (!inputBuilderFuture.isDone()) {
            return inputBuilderFuture;
        }
        if (outputBuilderFuture != null && !outputBuilderFuture.isDone()) {
            return outputBuilderFuture;
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            if (inputInMemoryGroupedTopNBuilder != null) {
                closer.register(inputInMemoryGroupedTopNBuilder::close);
            }

            if (outputInMemoryGroupedTopNBuilder != null) {
                closer.register(outputInMemoryGroupedTopNBuilder::close);
            }
            spiller.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillInProgress.isDone());
        if (inputInMemoryGroupedTopNBuilder.isEmpty()) {
            // All revocable memory has been released in buildResult method.
            // At this point, InMemoryGroupedTopNBuilder is no longer accepting any input so no point in spilling.
            return NOT_BLOCKED;
        }
        spillToDisk();
        return spillInProgress;
    }

    public void finishMemoryRevoke()
    {
        if (spiller.isPresent()) {
            checkState(spillInProgress.isDone());
            verify(inputInMemoryGroupedTopNBuilder.isEmpty());
            spiller.get().commit();
        }
    }

    @VisibleForTesting
    private WorkProcessor<Page> getFinalResult(List<WorkProcessor<Page>> sortedPageStreams)
    {
        MergeHashSort mergeHashSort = new MergeHashSort(aggregatedMemoryContextForMerge);
        WorkProcessor<Page> mergedSortedPages = mergeHashSort.merge(
                partitionTypes,
                partitionChannels,
                sourceTypes,
                sortedPageStreams,
                driverYieldSignal);

        initializeOutputInMemoryGroupedTopNBuilder();

        // Create final result by re-processing the sorted stream page-at-a-time through a GroupedTopNBuilder
        return mergedSortedPages.flatTransform(new WorkProcessor.Transformation<Page, WorkProcessor<Page>>()
        {
            public WorkProcessor.TransformationState<WorkProcessor<Page>> process(Optional<Page> inputPageOptional)
            {
                boolean inputIsPresent = inputPageOptional.isPresent();
                if (!inputIsPresent && outputInMemoryGroupedTopNBuilder.isEmpty()) {
                    // no more pages and builder is empty
                    return finished();
                }

                if (inputIsPresent) {
                    Page inputPage = inputPageOptional.get();
                    boolean done = outputInMemoryGroupedTopNBuilder.processPage(inputPage).process();
                    verify(done);
                    if (outputInMemoryGroupedTopNBuilder.getEstimatedSizeInBytes() < unspillMemoryLimit) {
                        return needsMoreData();
                    }
                }

                // We can produce output after every input page, because input pages do not have
                // hash values that span multiple pages (guaranteed by MergeHashSort)
                //
                // iterator to extract existing context out of builder
                WorkProcessor<Page> result = outputInMemoryGroupedTopNBuilder.buildResult();
                // initialize new builder
                initializeOutputInMemoryGroupedTopNBuilder();
                return ofResult(result, inputIsPresent);
            }
        });
    }

    private ListenableFuture<?> spillToDisk()
    {
        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(
                    sourceTypes,
                    spillContext,
                    aggregatedMemoryContextForSpill));
        }

        // start spilling process with current content of the inMemoryGroupedTopNBuilder builder...
        spillInProgress = spiller.get().spill(inputInMemoryGroupedTopNBuilder.buildHashSortedIntermediateResult());
        // ... and immediately create new inMemoryGroupedTopNBuilder so effectively memory ownership
        // over inMemoryGroupedTopNBuilder is transferred from this thread to a spilling thread
        initializeInputInMemoryGroupedTopNBuilder();
        updateMemoryReservations();

        return spillInProgress;
    }

    private void initializeInputInMemoryGroupedTopNBuilder()
    {
        if (inputInMemoryGroupedTopNBuilder != null) {
            inputInMemoryGroupedTopNBuilder.close();
        }
        inputInMemoryGroupedTopNBuilder = inputInMemoryGroupedTopNBuilderSupplier.get();
    }

    private void initializeOutputInMemoryGroupedTopNBuilder()
    {
        if (outputInMemoryGroupedTopNBuilder != null) {
            outputInMemoryGroupedTopNBuilder.close();
        }
        outputInMemoryGroupedTopNBuilder = outputInMemoryGroupedTopNBuilderSupplier.get();
    }

    @Override
    public Iterator<Page> buildHashSortedIntermediateResult()
    {
        throw new UnsupportedOperationException("SpillableGroupedTopNBuilder does not support buildHashSortedIntermediateResult");
    }

    @VisibleForTesting
    protected InMemoryGroupedTopNBuilder getInputInMemoryGroupedTopNBuilder()
    {
        return inputInMemoryGroupedTopNBuilder;
    }
}
