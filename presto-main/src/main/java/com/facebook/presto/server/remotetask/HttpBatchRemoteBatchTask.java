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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.http.client.thrift.ThriftRequestUtils;
import com.facebook.airlift.http.client.thrift.ThriftResponse;
import com.facebook.airlift.http.client.thrift.ThriftResponseHandler;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.execution.FutureStateChange;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap.NodeStatsTracker;
import com.facebook.presto.execution.PartitionedSplitsInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.BatchTaskUpdateRequest;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.facebook.airlift.http.client.HttpStatus.NO_CONTENT;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.server.RequestErrorTracker.isExpectedError;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.TaskResourceUtils.convertFromThriftTaskInfo;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.thrift.ThriftCodecWrapper.unwrapThriftCodec;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TASK_UPDATE_SIZE_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HttpBatchRemoteBatchTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpBatchRemoteBatchTask.class);
    private static final double UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE = 0.01;

    private final TaskId taskId;
    private URI taskLocation;
    private URI remoteTaskLocation;

    private final Session session;
    private String nodeId;
    private final PlanFragment planFragment;

    private final Set<PlanNodeId> tableScanPlanNodeIds;
    private final Set<PlanNodeId> remoteSourcePlanNodeIds;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final Duration maxErrorDuration;
    private final RemoteTaskStats stats;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoRefreshMaxWait;
    private final Duration taskInfoUpdateInterval;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Codec<MetadataUpdates> metadataUpdatesCodec;
    private final MetadataManager metadataManager;
    private final QueryManager queryManager;
    private TaskInfoFetcher taskInfoFetcher;
    private Optional<TaskResultFetcher> taskResultFetcher = Optional.empty();
    private ContinuousTaskStatusFetcher taskStatusFetcher;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private volatile long pendingSourceSplitsWeight;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    @GuardedBy("this")
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalLong whenSplitQueueHasSpaceThreshold = OptionalLong.empty();

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;

    private final Codec<TaskInfo> taskInfoCodec;
    //Json codec required for TaskUpdateRequest endpoint which uses JSON and returns a TaskInfo
    private final Codec<TaskInfo> taskInfoJsonCodec;
    // START - changed for batch
    private final Codec<BatchTaskUpdateRequest> batchTaskUpdateRequestCodec;
    // END - changed for batch
    private final Codec<PlanFragment> planFragmentCodec;

    private RequestErrorTracker updateErrorTracker;

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private NodeStatsTracker nodeStatsTracker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean aborting = new AtomicBoolean(false);

    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final boolean taskInfoThriftTransportEnabled;
    private final Protocol thriftProtocol;
    private final ConnectorTypeSerdeManager connectorTypeSerdeManager;
    private final HandleResolver handleResolver;
    private final int maxTaskUpdateSizeInBytes;
    private final int maxUnacknowledgedSplits;

    private final TableWriteInfo tableWriteInfo;

    private final DecayCounter taskUpdateRequestSize;

    private final Multimap<PlanNodeId, Split> initialSplits;

    private final LocationFactory locationFactory;

    private List<StateChangeListener> stateChangeListeners;

    private List<StateChangeListener> finalStageChangeListeners;

    private Optional<String> serializedShuffleWriteInfoOptional;

    public HttpBatchRemoteBatchTask(
            Session session,
            TaskId taskId,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            HttpClient httpClient,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            Codec<TaskStatus> taskStatusCodec,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskInfo> taskInfoJsonCodec,
            Codec<BatchTaskUpdateRequest> batchTaskUpdateRequestCodec,
            Codec<PlanFragment> planFragmentCodec,
            Codec<MetadataUpdates> metadataUpdatesCodec,
            RemoteTaskStats stats,
            boolean binaryTransportEnabled,
            boolean thriftTransportEnabled,
            boolean taskInfoThriftTransportEnabled,
            Protocol thriftProtocol,
            TableWriteInfo tableWriteInfo,
            int maxTaskUpdateSizeInBytes,
            MetadataManager metadataManager,
            QueryManager queryManager,
            DecayCounter taskUpdateRequestSize,
            HandleResolver handleResolver,
            ConnectorTypeSerdeManager connectorTypeSerdeManager,
            LocationFactory locationFactory)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(batchTaskUpdateRequestCodec, "batchTaskUpdateRequestCodec is null");
        requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(taskInfoRefreshMaxWait, "taskInfoRefreshMaxWait is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        requireNonNull(metadataManager, "metadataManager is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(thriftProtocol, "thriftProtocol is null");
        requireNonNull(handleResolver, "handleResolver is null");
        requireNonNull(connectorTypeSerdeManager, "connectorTypeSerdeManager is null");
        requireNonNull(taskUpdateRequestSize, "taskUpdateRequestSize cannot be null");
        requireNonNull(locationFactory, "locationFactory cannot be null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.session = session;
            this.planFragment = planFragment;
            this.initialSplits = initialSplits;
            this.httpClient = httpClient;
            this.executor = executor;
            this.locationFactory = locationFactory;
            this.updateScheduledExecutor = updateScheduledExecutor;
            this.errorScheduledExecutor = errorScheduledExecutor;
            this.taskStatusRefreshMaxWait = taskStatusRefreshMaxWait;
            this.taskInfoRefreshMaxWait = taskInfoRefreshMaxWait;
            this.taskInfoUpdateInterval = taskInfoUpdateInterval;
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskStatusCodec = taskStatusCodec;
            this.taskInfoCodec = taskInfoCodec;
            this.taskInfoJsonCodec = taskInfoJsonCodec;
            this.batchTaskUpdateRequestCodec = batchTaskUpdateRequestCodec;
            this.planFragmentCodec = planFragmentCodec;
            this.metadataUpdatesCodec = metadataUpdatesCodec;
            this.metadataManager = metadataManager;
            this.queryManager = queryManager;
            this.maxErrorDuration = maxErrorDuration;
            this.stats = stats;
            this.binaryTransportEnabled = binaryTransportEnabled;
            this.thriftTransportEnabled = thriftTransportEnabled;
            this.taskInfoThriftTransportEnabled = taskInfoThriftTransportEnabled;
            this.thriftProtocol = thriftProtocol;
            this.connectorTypeSerdeManager = connectorTypeSerdeManager;
            this.handleResolver = handleResolver;
            this.tableWriteInfo = tableWriteInfo;
            this.maxTaskUpdateSizeInBytes = maxTaskUpdateSizeInBytes;
            this.maxUnacknowledgedSplits = getMaxUnacknowledgedSplitsPerTask(session);
            checkArgument(maxUnacknowledgedSplits > 0, "maxUnacknowledgedSplits must be > 0, found: %s", maxUnacknowledgedSplits);

            this.tableScanPlanNodeIds = ImmutableSet.copyOf(planFragment.getTableScanSchedulingOrder());
            this.remoteSourcePlanNodeIds = planFragment.getRemoteSourceNodes().stream()
                    .map(PlanNode::getId)
                    .collect(toImmutableSet());
            this.taskUpdateRequestSize = taskUpdateRequestSize;

            for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getKey(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            int pendingSourceSplitCount = 0;
            long pendingSourceSplitsWeight = 0;
            for (PlanNodeId planNodeId : planFragment.getTableScanSchedulingOrder()) {
                Collection<Split> tableScanSplits = initialSplits.get(planNodeId);
                if (tableScanSplits != null && !tableScanSplits.isEmpty()) {
                    pendingSourceSplitCount += tableScanSplits.size();
                    pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, SplitWeight.rawValueSum(tableScanSplits, Split::getSplitWeight));
                }
            }
            this.pendingSourceSplitCount = pendingSourceSplitCount;
            this.pendingSourceSplitsWeight = pendingSourceSplitsWeight;

            this.stateChangeListeners = new ArrayList<>();
            this.finalStageChangeListeners = new ArrayList<>();

            this.serializedShuffleWriteInfoOptional = Optional.empty();
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public String getNodeId()
    {
        return nodeId;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfoFetcher.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return taskStatusFetcher.getTaskStatus();
    }

    @Override
    public Multimap<PlanNodeId, Split> getInitialSplits()
    {
        return initialSplits;
    }

    @Override
    public URI getRemoteTaskLocation()
    {
        return remoteTaskLocation;
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            started.set(true);
            try {
                scheduleUpdate();
            }
            catch (Exception ex) {
                failTask(new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to construct taskUpdate request"));
                return;
            }
            taskStatusFetcher.start();
            taskInfoFetcher.start();
            taskResultFetcher.ifPresent(TaskResultFetcher::start);
        }
    }

    @Override
    public RemoteTask assignToNode(InternalNode node, NodeStatsTracker nodeStatsTracker)
    {
        this.nodeId = node.getNodeIdentifier();
        // TODO (MRQueryScheduler) fix node stats tracker for HttpBatchRemoteTask
        //        this.nodeStatsTracker = requireNonNull(nodeStatsTracker, "nodeStatsTracker is null");
        this.taskLocation = locationFactory.createLegacyTaskLocation(node, taskId);
        this.remoteTaskLocation = locationFactory.createTaskLocation(node, taskId);
        this.updateErrorTracker = taskRequestErrorTracker(taskId, taskLocation, maxErrorDuration, errorScheduledExecutor, "updating task");

        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(taskLocation, "taskLocation is null");
        requireNonNull(remoteTaskLocation, "remoteTaskLocation is null");

        // setup output buffers
        this.outputBuffers.set(
                createInitialEmptyOutputBuffers(
                        planFragment.getPartitioningScheme().getPartitioning().getHandle()).withNoMoreBufferIds());
        requireNonNull(this.outputBuffers.get(), "outputBuffers is null");

        List<BufferInfo> bufferStates = new ArrayList<>();
        if (this.outputBuffers.get() != null) {
            bufferStates = this.outputBuffers.get().getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());
        }

        TaskInfo initialTask = createInitialTask(taskId, taskLocation, bufferStates, new TaskStats(DateTime.now(), null), nodeId);

        this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
                this::failTask,
                taskId,
                initialTask.getTaskStatus(),
                taskStatusRefreshMaxWait,
                taskStatusCodec,
                executor,
                httpClient,
                maxErrorDuration,
                errorScheduledExecutor,
                stats,
                binaryTransportEnabled,
                thriftTransportEnabled,
                thriftProtocol);

        this.taskInfoFetcher = new TaskInfoFetcher(
                this::failTask,
                initialTask,
                httpClient,
                taskInfoUpdateInterval,
                taskInfoRefreshMaxWait,
                taskInfoCodec,
                metadataUpdatesCodec,
                maxErrorDuration,
                summarizeTaskInfo,
                executor,
                updateScheduledExecutor,
                errorScheduledExecutor,
                stats,
                binaryTransportEnabled,
                taskInfoThriftTransportEnabled,
                session,
                metadataManager,
                queryManager,
                handleResolver,
                connectorTypeSerdeManager,
                thriftProtocol);

        if (!serializedShuffleWriteInfoOptional.isPresent()) {
            this.taskResultFetcher = Optional.of(new TaskResultFetcher(
                    httpClient,
                    updateScheduledExecutor,
                    node.getInternalUri(),
                    taskId,
                    errorScheduledExecutor,
                    executor,
                    maxErrorDuration));
        }

        if (!stateChangeListeners.isEmpty()) {
            for (StateChangeListener s : stateChangeListeners) {
                taskStatusFetcher.addStateChangeListener(s);
            }
            stateChangeListeners = null;
        }

        if (!finalStageChangeListeners.isEmpty()) {
            for (StateChangeListener s : finalStageChangeListeners) {
                taskInfoFetcher.addFinalTaskInfoListener(s);
            }
            finalStageChangeListeners = null;
        }

        taskStatusFetcher.addStateChangeListener(newStatus -> {
            TaskState state = newStatus.getState();
            if (state.isDone()) {
                cleanUpTask();
            }
            else {
                updateTaskStats();
                updateSplitQueueSpace();
            }
        });

        updateTaskStats();
        updateSplitQueueSpace();

        return this;
    }

    @Override
    public PlanFragment getPlanFragment()
    {
        return planFragment;
    }

    @Override
    public boolean isStarted()
    {
        return started.get();
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        boolean needsUpdate = false;
        for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            PlanNodeId sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();
            boolean isTableScanSource = tableScanPlanNodeIds.contains(sourceId);

            checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
            int added = 0;
            long addedWeight = 0;
            for (Split split : splits) {
                if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), sourceId, split))) {
                    if (isTableScanSource) {
                        added++;
                        addedWeight = addExact(addedWeight, split.getSplitWeight().getRawValue());
                    }
                }
            }
            if (isTableScanSource) {
                pendingSourceSplitCount += added;
                pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, addedWeight);
                updateTaskStats();
            }
            needsUpdate = true;
        }
        updateSplitQueueSpace();

        if (needsUpdate) {
            this.needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        if (noMoreSplits.containsKey(sourceId)) {
            return;
        }

        noMoreSplits.put(sourceId, true);
        needsUpdate.set(true);
        if (started.get()) {
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
    {
        if (pendingNoMoreSplitsForLifespan.put(sourceId, lifespan)) {
            needsUpdate.set(true);
            if (started.get()) {
                scheduleUpdate();
            }
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
            needsUpdate.set(true);
            if (started.get()) {
                scheduleUpdate();
            }
        }
    }

    public List<SerializedPage> getResults()
    {
        if (taskResultFetcher.isPresent()) {
            List<SerializedPage> result = new ArrayList<>();
            taskResultFetcher.get().extractResultInto(result);
            return result;
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ListenableFuture<?> removeRemoteSource(TaskId remoteSourceTaskId)
    {
        URI remoteSourceUri = uriBuilderFrom(taskLocation)
                .appendPath("remote-source")
                .appendPath(remoteSourceTaskId.toString())
                .build();

        Request request = prepareDelete()
                .setUri(remoteSourceUri)
                .build();
        RequestErrorTracker errorTracker = taskRequestErrorTracker(
                taskId,
                remoteSourceUri,
                maxErrorDuration,
                errorScheduledExecutor,
                "Remove exchange remote source");

        SettableFuture<?> future = SettableFuture.create();
        doRemoveRemoteSource(errorTracker, request, future);
        return future;
    }

    /// This method may call itself recursively when retrying for failures
    private void doRemoveRemoteSource(RequestErrorTracker errorTracker, Request request, SettableFuture<?> future)
    {
        errorTracker.startRequest();

        FutureCallback<StatusResponse> callback = new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse response)
            {
                if (response == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with null response");
                }
                if (response.getStatusCode() != OK.code() && response.getStatusCode() != NO_CONTENT.code()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with HTTP status " + response.getStatusCode());
                }
                future.set(null);
            }

            @Override
            public void onFailure(Throwable failedReason)
            {
                if (failedReason instanceof RejectedExecutionException && httpClient.isClosed()) {
                    log.error("Unable to destroy exchange source at %s. HTTP client is closed", request.getUri());
                    future.setException(failedReason);
                    return;
                }
                // record failure
                try {
                    errorTracker.requestFailed(failedReason);
                }
                catch (PrestoException e) {
                    future.setException(e);
                    return;
                }
                // if throttled due to error, asynchronously wait for timeout and try again
                ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
                if (errorRateLimit.isDone()) {
                    doRemoveRemoteSource(errorTracker, request, future);
                }
                else {
                    errorRateLimit.addListener(() -> doRemoveRemoteSource(errorTracker, request, future), errorScheduledExecutor);
                }
            }
        };

        addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), callback, directExecutor());
    }

    @Override
    public PartitionedSplitsInfo getPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus.getRunningPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight() + taskStatus.getRunningPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public PartitionedSplitsInfo getUnacknowledgedPartitionedSplitsInfo()
    {
        int count = pendingSourceSplitCount;
        long weight = pendingSourceSplitsWeight;
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public int getUnacknowledgedPartitionedSplitCount()
    {
        return getPendingSourceSplitCount();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    private long getQueuedPartitionedSplitsWeight()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitsWeight() + taskStatus.getQueuedPartitionedSplitsWeight();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private long getPendingSourceSplitsWeight()
    {
        return pendingSourceSplitsWeight;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            if (taskStatusFetcher == null) {
                stateChangeListeners.add(stateChangeListener);
            }
            else {
                taskStatusFetcher.addStateChangeListener(stateChangeListener);
            }
        }
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        if (taskStatusFetcher == null) {
            finalStageChangeListeners.add(stateChangeListener);
        }
        else {
            taskInfoFetcher.addFinalTaskInfoListener(stateChangeListener);
        }
    }

    @Override
    public synchronized ListenableFuture<?> whenSplitQueueHasSpace(long weightThreshold)
    {
        if (whenSplitQueueHasSpaceThreshold.isPresent()) {
            checkArgument(weightThreshold == whenSplitQueueHasSpaceThreshold.getAsLong(), "Multiple split queue space notification thresholds not supported");
        }
        else {
            whenSplitQueueHasSpaceThreshold = OptionalLong.of(weightThreshold);
            updateSplitQueueSpace();
        }
        if (splitQueueHasSpace) {
            return immediateFuture(null);
        }
        return whenSplitQueueHasSpace.createNewListener();
    }

    @Override
    public void setShuffleWriteInfo(String serializedShuffleWriteInfo)
    {
        this.serializedShuffleWriteInfoOptional = Optional.of(serializedShuffleWriteInfo);
    }

    private synchronized void updateSplitQueueSpace()
    {
        // Must check whether the unacknowledged split count threshold is reached even without listeners registered yet
        splitQueueHasSpace = getUnacknowledgedPartitionedSplitCount() < maxUnacknowledgedSplits &&
                (!whenSplitQueueHasSpaceThreshold.isPresent() || getQueuedPartitionedSplitsWeight() < whenSplitQueueHasSpaceThreshold.getAsLong());
        // Only trigger notifications if a listener might be registered
        if (splitQueueHasSpace && whenSplitQueueHasSpaceThreshold.isPresent()) {
            whenSplitQueueHasSpace.complete(null, executor);
        }
    }

    private void updateTaskStats()
    {
        if (nodeStatsTracker == null) {
            return;
        }

        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            nodeStatsTracker.setPartitionedSplits(PartitionedSplitsInfo.forZeroSplits());
            nodeStatsTracker.setMemoryUsage(0);
            nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis(), 0);
        }
        else {
            nodeStatsTracker.setPartitionedSplits(getPartitionedSplitsInfo());
            nodeStatsTracker.setMemoryUsage(taskStatus.getMemoryReservationInBytes() + taskStatus.getSystemMemoryReservationInBytes());
            nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis(), taskStatus.getTotalCpuTimeInNanos());
        }
    }

    private synchronized void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources)
    {
        //Setting the flag as false since TaskUpdateRequest is not on thrift yet.
        //Once it is converted to thrift we can use the isThrift enabled flag here.
        updateTaskInfo(newValue, false);

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            boolean isTableScanSource = tableScanPlanNodeIds.contains(planNodeId);
            int removed = 0;
            long removedWeight = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    if (isTableScanSource) {
                        removed++;
                        removedWeight = addExact(removedWeight, split.getSplit().getSplitWeight().getRawValue());
                    }
                }
            }
            if (source.isNoMoreSplits()) {
                noMoreSplits.put(planNodeId, false);
            }
            for (Lifespan lifespan : source.getNoMoreSplitsForLifespan()) {
                pendingNoMoreSplitsForLifespan.remove(planNodeId, lifespan);
            }
            if (isTableScanSource) {
                pendingSourceSplitCount -= removed;
                pendingSourceSplitsWeight -= removedWeight;
            }
        }
        // Update stats before split queue space to ensure node stats are up to date before waking up the scheduler
        updateTaskStats();
        updateSplitQueueSpace();
    }

    private void onSuccessTaskInfo(TaskInfo result)
    {
        try {
            updateTaskInfo(result, taskInfoThriftTransportEnabled);
        }
        finally {
            if (!getTaskInfo().getTaskStatus().getState().isDone()) {
                cleanUpLocally();
            }
        }
    }

    private void updateTaskInfo(TaskInfo taskInfo, boolean isTaskInfoThriftTransportEnabled)
    {
        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        if (isTaskInfoThriftTransportEnabled) {
            taskInfo = convertFromThriftTaskInfo(taskInfo, connectorTypeSerdeManager, handleResolver);
        }
        taskInfoFetcher.updateTaskInfo(taskInfo);
    }

    private void cleanUpLocally()
    {
        // Update the taskInfo with the new taskStatus.

        // Generally, we send a cleanup request to the worker, and update the TaskInfo on
        // the coordinator based on what we fetched from the worker. If we somehow cannot
        // get the cleanup request to the worker, the TaskInfo that we fetch for the worker
        // likely will not say the task is done however many times we try. In this case,
        // we have to set the local query info directly so that we stop trying to fetch
        // updated TaskInfo from the worker. This way, the task on the worker eventually
        // expires due to lack of activity.

        // This is required because the query state machine depends on TaskInfo (instead of task status)
        // to transition its own state.
        // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead

        // Since this TaskInfo is updated in the client the "complete" flag will not be set,
        // indicating that the stats may not reflect the final stats on the worker.
        updateTaskInfo(getTaskInfo().withTaskStatus(getTaskStatus()), taskInfoThriftTransportEnabled);
    }

    private void onFailureTaskInfo(
            Throwable t,
            String action,
            Request request,
            Backoff cleanupBackoff)
    {
        if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
            logError(t, "Unable to %s task at %s. HTTP client is closed.", action, request.getUri());
            cleanUpLocally();
            return;
        }

        // record failure
        if (cleanupBackoff.failure()) {
            logError(t, "Unable to %s task at %s. Back off depleted.", action, request.getUri());
            cleanUpLocally();
            return;
        }

        // reschedule
        long delayNanos = cleanupBackoff.getBackoffDelayNanos();
        if (delayNanos == 0) {
            doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
        }
        else {
            errorScheduledExecutor.schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
        }
    }

    private void scheduleUpdate()
    {
        sendUpdate();
    }

    private synchronized void sendUpdate()
    {
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task hasn't been started yet or if it is already finished
        if (!started.get() || !needsUpdate.get() || taskStatus.getState().isDone()) {
            return;
        }

        // if there is a request already running, wait for it to complete
        if (this.currentRequest != null && !this.currentRequest.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        List<TaskSource> sources = getSources();

        Optional<byte[]> fragment = sendPlan.get() ? Optional.of(planFragment.toBytes(planFragmentCodec)) : Optional.empty();
        Optional<TableWriteInfo> writeInfo = tableWriteInfo != null ? Optional.of(tableWriteInfo) : Optional.empty();

        // START - changed for Batch
        BatchTaskUpdateRequest batchTaskUpdateRequest = new BatchTaskUpdateRequest(
                new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers.get(),
                writeInfo),
                serializedShuffleWriteInfoOptional);
        byte[] batchTaskUpdateRequestJson = batchTaskUpdateRequestCodec.toBytes(batchTaskUpdateRequest);
        // END - changed for Batch
        taskUpdateRequestSize.add(batchTaskUpdateRequestJson.length);

        if (batchTaskUpdateRequestJson.length > maxTaskUpdateSizeInBytes) {
            failTask(new PrestoException(EXCEEDED_TASK_UPDATE_SIZE_LIMIT, format("TaskUpdate size of %d Bytes has exceeded the limit of %d Bytes", batchTaskUpdateRequestJson.length, maxTaskUpdateSizeInBytes)));
        }

        if (fragment.isPresent()) {
            stats.updateWithPlanSize(batchTaskUpdateRequestJson.length);
        }
        else {
            if (ThreadLocalRandom.current().nextDouble() < UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE) {
                // This is to keep track of the task update size even when the plan fragment is NOT present
                stats.updateWithoutPlanSize(batchTaskUpdateRequestJson.length);
            }
        }

        // START - changed for Batch
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).appendPath("/batch");
        // END - changed for Batch

        Request request = setContentTypeHeaders(binaryTransportEnabled, preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(batchTaskUpdateRequestJson))
                .build();

        ResponseHandler responseHandler;
        if (binaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler((JsonCodec<TaskInfo>) taskInfoJsonCodec);
        }

        updateErrorTracker.startRequest();

        ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
        // and does so without grabbing the instance lock.
        needsUpdate.set(false);

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR),
                executor);
    }

    private synchronized List<TaskSource> getSources()
    {
        return Stream.concat(tableScanPlanNodeIds.stream(), remoteSourcePlanNodeIds.stream())
                .map(this::getSource)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized TaskSource getSource(PlanNodeId planNodeId)
    {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean pendingNoMoreSplits = Boolean.TRUE.equals(this.noMoreSplits.get(planNodeId));
        boolean noMoreSplits = this.noMoreSplits.containsKey(planNodeId);
        Set<Lifespan> noMoreSplitsForLifespan = pendingNoMoreSplitsForLifespan.get(planNodeId);

        TaskSource element = null;
        if (!splits.isEmpty() || !noMoreSplitsForLifespan.isEmpty() || pendingNoMoreSplits) {
            element = new TaskSource(planNodeId, splits, noMoreSplitsForLifespan, noMoreSplits);
        }
        return element;
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            // send cancel to task and ignore response
            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).addParameter("abort", "false");
            Request.Builder builder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
            if (taskInfoThriftTransportEnabled) {
                builder = ThriftRequestUtils.prepareThriftDelete(thriftProtocol);
            }
            Request request = builder.setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cancel");
        }
    }

    private synchronized void cleanUpTask()
    {
        checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

        // clear pending splits to free memory
        pendingSplits.clear();
        pendingSourceSplitCount = 0;
        pendingSourceSplitsWeight = 0;
        updateTaskStats();
        splitQueueHasSpace = true;
        whenSplitQueueHasSpace.complete(null, executor);

        // cancel pending request
        if (currentRequest != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            currentRequest.cancel(false);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        taskStatusFetcher.stop();
        taskResultFetcher.ifPresent(resultFetcher -> resultFetcher.stop(false));

        // The remote task is likely to get a delete from the PageBufferClient first.
        // We send an additional delete anyway to get the final TaskInfo
        HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
        Request.Builder requestBuilder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
        if (taskInfoThriftTransportEnabled) {
            requestBuilder = ThriftRequestUtils.prepareThriftDelete(Protocol.BINARY);
        }
        Request request = requestBuilder
                .setUri(uriBuilder.build())
                .build();

        scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cleanup");
    }

    @Override
    public synchronized void abort()
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        abort(failWith(getTaskStatus(), ABORTED, ImmutableList.of()));
    }

    private synchronized void abort(TaskStatus status)
    {
        checkState(status.getState().isDone(), "cannot abort task with an incomplete status");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.updateTaskStatus(status);

            // send abort to task
            HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
            Request.Builder builder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
            if (taskInfoThriftTransportEnabled) {
                builder = ThriftRequestUtils.prepareThriftDelete(thriftProtocol);
            }

            Request request = builder.setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "abort");
        }
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        ResponseHandler responseHandler;
        if (taskInfoThriftTransportEnabled) {
            responseHandler = new ThriftResponseHandler(unwrapThriftCodec(taskInfoCodec));
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new ThriftResponseFutureCallback(action, request, cleanupBackoff),
                    executor);
        }
        else if (binaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new BaseResponseFutureCallback(action, request, cleanupBackoff),
                    executor);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler((JsonCodec<TaskInfo>) taskInfoCodec);
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new BaseResponseFutureCallback(action, request, cleanupBackoff),
                    executor);
        }
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private void failTask(Throwable cause)
    {
        TaskStatus taskStatus = getTaskStatus();
        if (!taskStatus.getState().isDone()) {
            log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
        }

        TaskStatus failedTaskStatus = failWith(getTaskStatus(), FAILED, ImmutableList.of(toFailure(cause)));
        // Transition task to failed state without waiting for the final task info returned by the abort request.
        // The abort request is very likely not to succeed, leaving the task and the stage in the limbo state for
        // the entire duration of abort retries. If the task is failed, it is not that important to actually
        // record the final statistics and the final information about a failed task.
        taskInfoFetcher.updateTaskInfo(getTaskInfo().withTaskStatus(failedTaskStatus));

        // Initiate abort request
        abort(failedTaskStatus);
    }

    private HttpUriBuilder getHttpUriBuilder(TaskStatus taskStatus)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(taskStatus.getSelf());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        return uriBuilder;
    }

    private static Backoff createCleanupBackoff()
    {
        return new Backoff(10, new Duration(10, TimeUnit.MINUTES), Ticker.systemTicker(), ImmutableList.<Duration>builder()
                .add(new Duration(0, MILLISECONDS))
                .add(new Duration(100, MILLISECONDS))
                .add(new Duration(500, MILLISECONDS))
                .add(new Duration(1, SECONDS))
                .add(new Duration(10, SECONDS))
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final List<TaskSource> sources;

        private UpdateResponseHandler(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long currentRequestStartNanos;
                    synchronized (HttpBatchRemoteBatchTask.this) {
                        currentRequest = null;
                        sendPlan.set(value.isNeedsPlan());
                        currentRequestStartNanos = HttpBatchRemoteBatchTask.this.currentRequestStartNanos;
                    }
                    updateStats(currentRequestStartNanos);
                    processTaskUpdate(value, sources);
                    updateErrorTracker.requestSucceeded();
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    long currentRequestStartNanos;
                    synchronized (HttpBatchRemoteBatchTask.this) {
                        currentRequest = null;
                        currentRequestStartNanos = HttpBatchRemoteBatchTask.this.currentRequestStartNanos;
                    }
                    updateStats(currentRequestStartNanos);

                    // on failure assume we need to update again
                    needsUpdate.set(true);

                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    failTask(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    failTask(e);
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                failTask(cause);
            }
        }

        private void updateStats(long currentRequestStartNanos)
        {
            Duration requestRoundTrip = Duration.nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
        }
    }

    private static void logError(Throwable t, String format, Object... args)
    {
        if (isExpectedError(t)) {
            log.error(format + ": %s", ObjectArrays.concat(args, t));
        }
        else {
            log.error(t, format, args);
        }
    }

    private class ThriftResponseFutureCallback
            implements FutureCallback<ThriftResponse<TaskInfo>>
    {
        private final String action;
        private final Request request;
        private final Backoff cleanupBackoff;

        public ThriftResponseFutureCallback(String action, Request request, Backoff cleanupBackoff)
        {
            this.action = action;
            this.request = request;
            this.cleanupBackoff = cleanupBackoff;
        }

        @Override
        public void onSuccess(ThriftResponse<TaskInfo> result)
        {
            onSuccessTaskInfo(result.getValue());
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            onFailureTaskInfo(throwable, this.action, this.request, this.cleanupBackoff);
        }
    }

    private class BaseResponseFutureCallback
            implements FutureCallback<BaseResponse<TaskInfo>>
    {
        private final String action;
        private final Request request;
        private final Backoff cleanupBackoff;

        public BaseResponseFutureCallback(String action, Request request, Backoff cleanupBackoff)
        {
            this.action = action;
            this.request = request;
            this.cleanupBackoff = cleanupBackoff;
        }

        @Override
        public void onSuccess(BaseResponse<TaskInfo> result)
        {
            onSuccessTaskInfo(result.getValue());
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            onFailureTaskInfo(throwable, this.action, this.request, this.cleanupBackoff);
        }
    }
}
