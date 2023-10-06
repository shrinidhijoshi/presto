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
package com.facebook.presto.execution.scheduler.mapreduce;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageExecutionStateMachine;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.ExchangeLocationsConsumer;
import com.facebook.presto.execution.scheduler.ScheduleResult;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.execution.scheduler.mapreduce.exchange.ExchangeDependency;
import com.facebook.presto.execution.scheduler.mapreduce.exchange.ExchangeProviderRegistry;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.exchange.ExchangeProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableCommitMetadataSourceNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.execution.StageExecutionState.PLANNED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_RECOVERY_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MRStageExecution
{
    private static final Logger log = Logger.get(MRStageExecution.class);
    public static final Set<ErrorCode> RECOVERABLE_ERROR_CODES = ImmutableSet.of(
            TOO_MANY_REQUESTS_FAILED.toErrorCode(),
            PAGE_TRANSPORT_ERROR.toErrorCode(),
            PAGE_TRANSPORT_TIMEOUT.toErrorCode(),
            REMOTE_TASK_ERROR.toErrorCode());
    private static final int MAX_TASK_RETRIES = 4;

    private final Session session;
    private final StageExecutionStateMachine stateMachine;
    private final PlanFragment planFragment;
    private final RemoteTaskFactory remoteTaskFactory;
    private final boolean summarizeTaskInfo;
    private final TableWriteInfo tableWriteInfo;
    @GuardedBy("this")
    private final Map<TaskId, RemoteTask> allTasks = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Set<RemoteTask> scheduledTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> tasksWithFinalInfo = newConcurrentHashSet();
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final PartitioningProviderManager partitioningProviderManager;
    private final SplitSourceFactory splitSourceFactory;
    private final MRTaskScheduler mrTaskScheduler;
    private final LinkedBlockingDeque<RemoteTask> pendingTasks;

    private final MRTableCommitMetadataCache mrResultCache;
    private final ExchangeProvider exchangeProvider;
    private TaskOutputTracker taskOutputTracker;
    private final ExchangeLocationsConsumer exchangeLocationsConsumer;
    private final List<ExchangeDependency> exchangeDependencies;
    private final ExchangeDependency exchangeOutput;
    private Map<Integer, ListMultimap<PlanNodeId, Split>> partitionedSplits;
    private Map<TaskId, Integer> taskIndexes;
    private AtomicInteger currentTaskIndex;
    private String exchangeId = "";

    public static MRStageExecution createStageExecution(
            StageExecutionId stageExecutionId,
            PlanFragment fragment,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            OutputBuffers outputBuffers,
            ExchangeLocationsConsumer exchangeLocationsConsumer,
            boolean summarizeTaskInfo,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            TableWriteInfo tableWriteInfo,
            PartitioningProviderManager partitioningProviderManager,
            SplitSourceFactory splitSourceFactory,
            MRTaskScheduler mrTaskScheduler,
            MRTableCommitMetadataCache mrResultCache,
            ExchangeProviderRegistry exchangeProviderRegistry,
            List<ExchangeDependency> exchangeDependencies)
    {
        requireNonNull(stageExecutionId, "stageId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        requireNonNull(session, "session is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(schedulerStats, "schedulerStats is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        requireNonNull(exchangeProviderRegistry, "shuffleManager is null");

        MRStageExecution stageExecution = new MRStageExecution(
                session,
                new StageExecutionStateMachine(stageExecutionId, executor, schedulerStats, !fragment.getTableScanSchedulingOrder().isEmpty()),
                fragment,
                remoteTaskFactory,
                outputBuffers,
                exchangeLocationsConsumer,
                summarizeTaskInfo,
                executor,
                tableWriteInfo,
                partitioningProviderManager,
                splitSourceFactory,
                mrTaskScheduler,
                mrResultCache,
                exchangeProviderRegistry,
                exchangeDependencies);
        return stageExecution;
    }

    private MRStageExecution(
            Session session,
            StageExecutionStateMachine stateMachine,
            PlanFragment planFragment,
            RemoteTaskFactory remoteTaskFactory,
            OutputBuffers outputBuffers,
            ExchangeLocationsConsumer exchangeLocationsConsumer,
            boolean summarizeTaskInfo,
            Executor executor,
            TableWriteInfo tableWriteInfo,
            PartitioningProviderManager partitioningProviderManager,
            SplitSourceFactory splitSourceFactory,
            MRTaskScheduler mrTaskScheduler,
            MRTableCommitMetadataCache mrResultCache,
            ExchangeProviderRegistry exchangeProviderRegistry,
            List<ExchangeDependency> exchangeDependencies)
    {
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = stateMachine;
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.tableWriteInfo = requireNonNull(tableWriteInfo);
        this.exchangeProvider = exchangeProviderRegistry.get("file");
        this.exchangeDependencies = exchangeDependencies;

        ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> fragmentToExchangeSource = ImmutableMap.builder();
        for (RemoteSourceNode remoteSourceNode : planFragment.getRemoteSourceNodes()) {
            for (PlanFragmentId planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                fragmentToExchangeSource.put(planFragmentId, remoteSourceNode);
            }
        }
        this.partitioningProviderManager = partitioningProviderManager;
        this.splitSourceFactory = splitSourceFactory;
        this.mrTaskScheduler = mrTaskScheduler;
        this.pendingTasks = new LinkedBlockingDeque<>();
        this.mrResultCache = mrResultCache;
        this.outputBuffers.set(outputBuffers);
        this.exchangeLocationsConsumer = exchangeLocationsConsumer;
        this.taskIndexes = new HashMap<>();
        this.currentTaskIndex = new AtomicInteger();

        // Step1: get partitioned splits
        // TODO: Can we do lazy fetching to keep memory pressure low on coordinator ?
        partitionedSplits = getPartitionedSplits(planFragment, splitSourceFactory, session, tableWriteInfo);

        // Step2: register job for shuffle (if applicable)
        PartitioningScheme outputPartitioning = planFragment.getPartitioningScheme();
        int outputPartitionCount;
        if (outputPartitioning.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION)
                || (outputPartitioning.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION))
                || (outputPartitioning.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION))) {
            outputPartitionCount = 1;
        }
        else if (outputPartitioning.getBucketToPartition().isPresent()) {
            outputPartitionCount = outputPartitioning.getBucketToPartition().get().length;
        }
        else {
            throw new RuntimeException(format("Unknown outputPartitioning %s for fragment %s",
                    outputPartitioning.getPartitioning().getHandle(),
                    planFragment.getId()));
        }

        // create exchange output to be used for DAG construction
        String exchangeId = stateMachine.getStageExecutionId().toString()
                .replace(".", "_");
        exchangeOutput = new ExchangeDependency(
                exchangeId,
                partitionedSplits.keySet().size(),
                outputPartitionCount,
                planFragment,
                exchangeProvider);
        this.exchangeId = exchangeId;

        // create task output tracker to track task outputs
        // along with metadata needed by shuffle system
        taskOutputTracker = new TaskOutputTracker(
                partitionedSplits.keySet().size(),
                (taskInfo) -> exchangeProvider.generateExchangeWriteOutputMetadata(exchangeId, taskIndexes.get(taskInfo.getTaskId())));
    }

    public void schedule()
    {
        // stage is complete nothing to do.
        if (stateMachine.getState().isDone()) {
            return;
        }

        // Tasks in the stage need to be scheduled.
        // Create and queue them
        if (stateMachine.getState().equals(PLANNED)) {
            stateMachine.transitionToScheduling();

            Set<RemoteTask> newlyScheduledTasks = new HashSet<>();

            // Step1: For each partition create a task with the splits for that partition
            for (Map.Entry<Integer, ListMultimap<PlanNodeId, Split>> splitsForPartition : partitionedSplits.entrySet()) {
                RemoteTask task = createTask(
                        splitsForPartition.getKey(),
                        0,
                        splitsForPartition.getValue());

                // set noMoreSplits for tableRead and shuffleRead
                planFragment.getTableScanSchedulingOrder()
                        .forEach(task::noMoreSplits);
                planFragment.getRemoteSourceNodes().stream().map(PlanNode::getId)
                        .forEach(task::noMoreSplits);

                // track it locally
                allTasks.put(task.getTaskId(), task);

                // schedule task into queue
                pendingTasks.add(task);
                newlyScheduledTasks.add(task);
            }

            ScheduleResult result = ScheduleResult.nonBlocked(true, newlyScheduledTasks, newlyScheduledTasks.stream()
                    .mapToInt(task -> task.getInitialSplits().values().size()).sum());

            // For coordinator tasks (ex: TableFinish) we do not
            // need to register with Task scheduler.
            // Rather we ask the node (coordinator) and assign
            // and run it directly ourselves
            // In the future, if needed we can make task scheduler
            // aware of node spec and assign correct tasks to
            // different type of nodes.
            if (planFragment.getPartitioning().isCoordinatorOnly()) {
                InternalNode coordinator = mrTaskScheduler.getCoordinator();
                pendingTasks.forEach(coordinatorTask -> {
                    coordinatorTask.assignToNode(coordinator, null);
                    coordinatorTask.start();
                    scheduledTasks.add(coordinatorTask);
                });
            }
            else {
                mrTaskScheduler.registerStageExecution(this);
            }

            if (result.isFinished()) {
                stateMachine.transitionToScheduled();
            }
        }
        else if (stateMachine.getState().equals(SCHEDULED) || stateMachine.getState().equals(RUNNING)) {
            Iterator<RemoteTask> scheduledTasksIterator = scheduledTasks.iterator();
            while (scheduledTasksIterator.hasNext()) {
                RemoteTask remoteTask = scheduledTasksIterator.next();
                // task is still in queue, nothing to do
                if (!remoteTask.isStarted() || !remoteTask.getTaskStatus().getState().isDone()) {
                    return;
                }

                TaskStatus taskStatus = remoteTask.getTaskStatus();
                TaskId taskId = remoteTask.getTaskId();
                if (taskStatus.getState() == TaskState.RUNNING) {
                    //TODO(MRScheduler): Update the stage statistics
                }
                else if (taskStatus.getState() == TaskState.FINISHED) {
                    if (remoteTask.getTaskInfo().getTaskStatus().getState() == remoteTask.getTaskStatus().getState()) {
                        // update task tracking
                        scheduledTasksIterator.remove();
                        finishedTasks.add(taskId);

                        boolean success = taskOutputTracker.tryCommit(remoteTask.getTaskInfo());
                        // update results if applicable
                        if (success && searchFrom(planFragment.getRoot())
                                .where(TableWriterNode.class::isInstance)
                                .findFirst().isPresent()) {
                            mrResultCache.putResultsForTask(remoteTask.getTaskInfo().getTaskId().getStageExecutionId(), remoteTask.getResults());
                        }
                    }
                }
                else if (taskStatus.getState() == TaskState.FAILED) {
                    // update task tracking
                    scheduledTasksIterator.remove();
                    finishedTasks.add(taskId);

                    RuntimeException failure = taskStatus.getFailures().stream()
                            .findFirst()
                            .map(MRStageExecution::rewriteTransportFailure)
                            .map(ExecutionFailureInfo::toException)
                            .orElse(new PrestoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason"));

                    if (isRecoverableTaskFailure(taskStatus.getFailures())) {
                        try {
                            // Queue a retry for this task
                            RemoteTask failedTask = allTasks.get(taskId);
                            if (taskId.getAttemptNumber() + 1 == MAX_TASK_RETRIES) {
                                // we have run out of retries
                                // fail the stage
                                throw new RuntimeException(format("Exceeded allowed number of retries for taskId = %s", taskId.getId()));
                            }
                            RemoteTask task = createTask(
                                    taskId.getId(),
                                    taskId.getAttemptNumber() + 1,
                                    failedTask.getInitialSplits());

                            // set noMoreSplits for tableRead and shuffleRead
                            planFragment.getTableScanSchedulingOrder().forEach(task::noMoreSplits);
                            planFragment.getRemoteSourceNodes().stream().map(PlanNode::getId).forEach(task::noMoreSplits);

                            // track it locally
                            allTasks.put(task.getTaskId(), task);
                            scheduledTasks.add(task);
                            taskIndexes.put(task.getTaskId(), currentTaskIndex.incrementAndGet());

                            /// schedule task into queue
                            pendingTasks.add(task);
                        }
                        catch (Throwable t) {
                            // In an ideal world, this exception is not supposed to happen.
                            // However, it could happen, for example, if connector throws exception.
                            // We need to handle the exception in order to fail the query properly, otherwise the failed task will hang in RUNNING/SCHEDULING state.
                            failure.addSuppressed(new PrestoException(GENERIC_RECOVERY_ERROR, format("Encountered error when trying to recover task %s", taskId), t));
                            stateMachine.transitionToFailed(failure);
                        }
                    }
                    else {
                        stateMachine.transitionToFailed(failure);
                    }
                }
                else if (taskStatus.getState() == TaskState.ABORTED) {
                    // update task tracking
                    scheduledTasksIterator.remove();
                    finishedTasks.add(taskId);

                    // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                    stateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "A task is in the ABORTED state but stage is " + stateMachine.getState()));
                }
            }

            // Check if all tasks in the stage are complete
            // This is the only sure shot way of knowing
            // that the stage is complete
            if (taskOutputTracker.isAllCommitted()) {
                // Kill all remaining tasks as they are
                // duplicates
                allTasks.values().forEach(remoteTask -> {
                    if (!remoteTask.isStarted() || !remoteTask.getTaskStatus().getState().isDone()) {
                        // Before killing, double check if task
                        // declared running are actually previously committed
                        // Fail the stage execution if such a discrepancy is found
                        Set<TaskInfo> badTasks = taskOutputTracker.getAllTaskInfos().stream()
                                .filter(taskInfo -> taskInfo.getTaskId().equals(remoteTask.getTaskId()))
                                .collect(Collectors.toSet());
                        checkArgument(badTasks.isEmpty(), "Some tasks were committed but are still running. Fatal.");

                        // All is good. Abort this duplicate task
                        remoteTask.abort();
                    }
                });

                // Now we are done with this stage
                stateMachine.transitionToFinished();

                // check if all tasks are done
                stateMachine.setAllTasksFinal(taskOutputTracker.getAllTaskInfos(), 0);
            }
        }
    }

    public Optional<MRTaskScheduler.ResourceSlot> considerSlotOffer(MRTaskScheduler.ResourceSlot slot)
    {
        if (stateMachine.getState().isDone() || pendingTasks.size() == 0) {
            return Optional.of(slot);
        }

        RemoteTask pendingTask = pendingTasks.poll();
        // it is possible that between the size check and when we start
        // polling from the queue, some other thread (considerSlotOffer)
        // already cleared the task. Applies when last task is being picked
        // from the queue.
        // Do one more check if we actually got a task
        if (pendingTask == null) {
            return Optional.of(slot);
        }
        // Try to occupy the slot. It's possible
        // that this slot is not free anymore
        // Ideally this shouldn't happen based on
        // current impl. But it's possible to change
        // how we distribute slots to stages, so
        // ensuring that slot occupying code is
        // thread safe is necessary
        if (!slot.occupy(pendingTask)) {
            log.error("Trying to schedule task into occupied slot");
            return Optional.of(slot);
        }

        // We have control of the slot now.
        // Try to assign the node to this task
        // and try to start the task
        try {
            // Try to assign the task to that slot
            // compute remaining task details based on node info
            pendingTask.assignToNode(
                    slot.getInternalNode(),
                    null);
            // try starting the task
            pendingTask.start();
            scheduledTasks.add(pendingTask);
            log.info("Assigned task %s to node %s",
                    pendingTask.getTaskId(), slot.getInternalNode().getNodeIdentifier());

            // if this is the first task, transition state SCHEDULED -> RUNNING
            if (stateMachine.getState() == SCHEDULED) {
                stateMachine.transitionToRunning();
            }
        }
        catch (Exception ex) {
            log.error("Exception trying to assign the task %s to slot. Will put it back to queue",
                    pendingTask.getTaskId());
            slot.free();
            return Optional.of(slot);
        }
        return Optional.empty();
    }

    public synchronized void cancel()
    {
        getAllTasks().forEach(RemoteTask::cancel);
        stateMachine.transitionToCanceled();
    }

    public synchronized void abort()
    {
        getAllTasks().forEach(RemoteTask::abort);
        stateMachine.transitionToAborted();
    }

    public StageExecutionId getStageExecutionId()
    {
        return stateMachine.getStageExecutionId();
    }

    public StageExecutionState getState()
    {
        return stateMachine.getState();
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addStateChangeListener(StateMachine.StateChangeListener<StageExecutionState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateMachine.StateChangeListener<StageExecutionInfo> stateChangeListener)
    {
        stateMachine.addFinalStageInfoListener(stateChangeListener);
    }

    public PlanFragment getFragment()
    {
        return planFragment;
    }

    public ExchangeDependency getExchangeOutput()
    {
        return exchangeOutput;
    }

    public OutputBuffers getOutputBuffers()
    {
        return outputBuffers.get();
    }

    public long getUserMemoryReservation()
    {
        return stateMachine.getUserMemoryReservation();
    }

    public long getTotalMemoryReservation()
    {
        return stateMachine.getTotalMemoryReservation();
    }

    public Duration getTotalCpuTime()
    {
        long millis = getAllTasks().stream()
                .filter(RemoteTask::isStarted)
                .mapToLong(task -> NANOSECONDS.toMillis(task.getTaskInfo().getStats().getTotalCpuTimeInNanos()))
                .sum();
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public synchronized DataSize getRawInputDataSize()
    {
        if (planFragment.getTableScanSchedulingOrder().isEmpty()) {
            return new DataSize(0, BYTE);
        }
        long datasize = getAllTasks().stream()
                .filter(RemoteTask::isStarted)
                .mapToLong(task -> task.getTaskInfo().getStats().getRawInputDataSizeInBytes())
                .sum();
        return DataSize.succinctBytes(datasize);
    }

    public BasicStageExecutionStats getBasicStageStats()
    {
        return stateMachine.getBasicStageStats(this::getAllTaskInfo);
    }

    public StageExecutionInfo getStageExecutionInfo()
    {
        // TODO (MRScheduler): finished and totalLifeSpanCount
        return stateMachine.getStageExecutionInfo(this::getAllTaskInfo, 0, 0);
    }

    private Iterable<TaskInfo> getAllTaskInfo()
    {
        return getAllTasks().stream()
                .filter(RemoteTask::isStarted)
                .map(RemoteTask::getTaskInfo)
                .collect(toImmutableList());
    }

    // do not synchronize
    // this is used for query info building which should be independent of scheduling work
    public boolean hasTasks()
    {
        // TODO (MRScheduler) Currently assume that when first task
        // starts running, we mark stage as running.
        return !allTasks.isEmpty();
    }

    // do not synchronize
    // this is used for query info building which should be independent of scheduling work
    public List<RemoteTask> getAllTasks()
    {
        return allTasks.values().stream().collect(Collectors.toList());
    }

    private Map<Integer, ListMultimap<PlanNodeId, Split>> getPartitionedSplits(
            PlanFragment planFragment,
            SplitSourceFactory splitSourceFactory,
            Session session,
            TableWriteInfo tableWriteInfo)
    {
        int totalNumberOfSplits = 0;
        AtomicLong nextSplitId = new AtomicLong();
        TaskId dummyTaskId = TaskId.valueOf("dummy.0.0.0.0");

        // read the fragment and create tasks out of it
        List<PlanNodeId> tableScanPlanNodeIds = searchFrom(planFragment.getRoot())
                .where(TableScanNode.class::isInstance)
                .findAll().stream().map(PlanNode::getId).collect(Collectors.toList());

        Map<Integer, ListMultimap<PlanNodeId, Split>> partitionedSplits = new HashMap<>();

        // Add splits for TableScan nodes
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(planFragment, session, tableWriteInfo);
        for (PlanNodeId tableScanPlanNodeId : tableScanPlanNodeIds) {
            SplitSource splitSource = requireNonNull(splitSources.get(tableScanPlanNodeId),
                    "split source is missing for table scan node with id: " + tableScanPlanNodeId);
            try (SplitAssigner splitAssigner = createSplitAssigner(session, tableScanPlanNodeId, splitSource, planFragment.getPartitioning())) {
                while (true) {
                    Optional<SetMultimap<Integer, ScheduledSplit>> batch = splitAssigner.getNextBatch();
                    if (!batch.isPresent()) {
                        break;
                    }
                    int numberOfSplitsInCurrentBatch = batch.get().size();
                    totalNumberOfSplits += numberOfSplitsInCurrentBatch;

                    SetMultimap<Integer, ScheduledSplit> partitionedSplitsForNode = batch.get();
                    for (int partitionId : ImmutableSet.copyOf(partitionedSplitsForNode.keySet())) {
                        // compute splits for this partition (for the given tablescan node)
                        List<Split> splits = partitionedSplitsForNode.removeAll(partitionId).stream()
                                .map(ScheduledSplit::getSplit)
                                .collect(Collectors.toList());

                        // find the partitioned entry in final map to put the computed splits
                        partitionedSplits
                                .compute(partitionId, (_partitionId, _splits) -> {
                                    if (_splits == null) {
                                        _splits = ArrayListMultimap.create();
                                    }
                                    _splits.putAll(tableScanPlanNodeId, splits);
                                    return _splits;
                                });
                    }
                }
            }
            log.info("Total number of splits for table scan node with id %s: %s", tableScanPlanNodeId, totalNumberOfSplits);
        }

        // Add splits for ShuffleRead nodes
        List<RemoteSourceNode> shuffleReadPlanNodes = searchFrom(planFragment.getRoot())
                .where(RemoteSourceNode.class::isInstance)
                .findAll();

        for (RemoteSourceNode exchangeReadNode : shuffleReadPlanNodes) {
            // Add shuffle read info
            HashMultimap<Integer, ScheduledSplit> splits = HashMultimap.create();

            // Find the source node id from which to read the shuffle splits
            checkArgument(exchangeReadNode.getSourceFragmentIds().stream().findFirst().isPresent());
            PlanFragmentId sourceFragmentId = exchangeReadNode.getSourceFragmentIds().stream().findFirst().get();
            Optional<ExchangeDependency> sourceExchangeDependency = exchangeDependencies.stream()
                    .filter(exchangeDependency -> exchangeDependency.getWriterFragment().getId() == sourceFragmentId)
                    .findFirst();
            checkArgument(sourceExchangeDependency.isPresent(), "exchangeDependency not found for exchangeReadNode");
            String sourceExchangeId = sourceExchangeDependency.get().getExchangeId();

            // For each partition create the split locations of previous stage shuffle write
            int numPartitions;
            ExchangeNode.Type shuffleDependencyType = exchangeReadNode.getExchangeType();
            if (planFragment.getPartitioning().equals(SINGLE_DISTRIBUTION) || planFragment.getPartitioning().equals(FIXED_ARBITRARY_DISTRIBUTION)) {
                numPartitions = 1;
            }
            else if (planFragment.getPartitioning().equals(FIXED_HASH_DISTRIBUTION)) {
                numPartitions = getHashPartitionCount(session);
            }
            else if (planFragment.getPartitioning().equals(SOURCE_DISTRIBUTION)) {
                checkArgument(tableScanPlanNodeIds.size() == 1,
                        format("If there is a shuffle, then only 1 table scan is allowed in the fragment but found %s", tableScanPlanNodeIds.size()));
                checkArgument(shuffleDependencyType.equals(REPLICATE),
                        format("TableScan and shuffle read only support for broadcast read, but found %s", shuffleDependencyType));
                numPartitions = partitionedSplits.size();
            }
            else {
                throw new UnsupportedOperationException(format("Unknown partitioning handle for child stage: %s", planFragment.getPartitioning()));
            }

            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                List<Integer> partitionIdsToRead;
                if (shuffleDependencyType.equals(REPLICATE)) {
                    // For broadcast join, all shuffleRead nodes read the same partition (0)
                    partitionIdsToRead = ImmutableList.of(0);
                }
                else {
                    partitionIdsToRead = ImmutableList.of(partitionId);
                }

                splits.put(partitionId, new ScheduledSplit(
                        nextSplitId.getAndIncrement(),
                        exchangeReadNode.getId(),
                        new Split(
                                REMOTE_CONNECTOR_ID,
                                new RemoteTransactionHandle(),
                                new RemoteSplit(
                                        new Location(format("batch://%s?shuffleInfo=%s", dummyTaskId, exchangeProvider.generateExchangeReadMetadata(
                                                // parent stage execution id. We assume that stageAttemptId=0 as there are no stage level retries
                                                // write a better utility to convert StageExecutionId to ShuffleId
                                                sourceExchangeId,
                                                partitionIdsToRead,
                                                ImmutableList.of()))),
                                        dummyTaskId))));
            }

            for (int partitionId : ImmutableSet.copyOf(splits.keySet())) {
                // remove the entry from the collection to let GC reclaim the memory
                List<Split> exchangeSplits = splits.removeAll(partitionId).stream()
                        .map(ScheduledSplit::getSplit).collect(Collectors.toList());
                // find the partitioned entry in final map to put the computed splits
                partitionedSplits
                        .compute(partitionId, (_partitionId, _splits) -> {
                            if (_splits == null) {
                                _splits = ArrayListMultimap.create();
                            }
                            _splits.putAll(exchangeReadNode.getId(), exchangeSplits);
                            return _splits;
                        });
            }
            log.info("Total number of splits for ShuffleReadNode with id %s: %s", exchangeReadNode.getId(), totalNumberOfSplits);
        }

        // Add splits for TableCommitMetadata if applicable
        if (planFragment.getPartitioning().isCoordinatorOnly()) {
            checkArgument(partitionedSplits.isEmpty(), "Found input splits for COORDINATOR_ONLY fragment");
            List<PlanNode> tableCommitMetadataSourceNodes = searchFrom(planFragment.getRoot())
                    .where(TableCommitMetadataSourceNode.class::isInstance)
                            .findAll();

            ImmutableListMultimap.Builder<PlanNodeId, Split> nodeSourcesBuilder = ImmutableListMultimap.builder();
            tableCommitMetadataSourceNodes.forEach(tableCommitMetadataSourceNode -> {
                nodeSourcesBuilder.put(
                        tableCommitMetadataSourceNode.getId(),
                                new Split(
                                new ConnectorId("$localResultCache"),
                                new RemoteTransactionHandle(),
                                new RemoteSplit(new Location("dummyLocation"), dummyTaskId)));
            });
            partitionedSplits.put(0, nodeSourcesBuilder.build());
        }

        return partitionedSplits;
    }

    private boolean isRecoverableTaskFailure(List<ExecutionFailureInfo> failures)
    {
        return failures.stream().anyMatch(executionFailureInfo ->
            RECOVERABLE_ERROR_CODES.contains(executionFailureInfo.getErrorCode()));
    }

    private RemoteTask createTask(int partitionNumber, int attemptNumber, Multimap<PlanNodeId, Split> initialSplits)
    {
        TaskId taskId = new TaskId(stateMachine.getStageExecutionId(), partitionNumber, attemptNumber);
        taskIndexes.put(taskId, currentTaskIndex.incrementAndGet());

        RemoteTask task;
        if (planFragment.getPartitioning().isCoordinatorOnly()) {
            task = remoteTaskFactory.createRemoteTask(
                    session,
                    taskId,
                    planFragment,
                    initialSplits,
                    summarizeTaskInfo,
                    tableWriteInfo);
            task.setOutputBuffers(this.outputBuffers.get());

            // Coordinator only tasks are single tasks
            // so we can set the noMoreExchangeLocations to
            task.addStateChangeListener((taskStatus) -> {
                if (taskStatus.getState() == TaskState.RUNNING) {
                    // add outputlocations. Only used in the case of output from root node
                    // internally it is a no-op for non-root nodes
                    exchangeLocationsConsumer.addExchangeLocations(planFragment.getId(), ImmutableSet.of(task), true);
                }
            });
            searchFrom(planFragment.getRoot()).where(TableCommitMetadataSourceNode.class::isInstance).findAll()
                    .forEach(planNode -> {
                        task.noMoreSplits(planNode.getId());
                    });
        }
        else {
            task = remoteTaskFactory.createRemoteBatchTask(
                    session,
                    taskId,
                    planFragment,
                    initialSplits,
                    summarizeTaskInfo,
                    tableWriteInfo);
            // set ShuffleWriteInfo if it's not tableWriteNode or RootNode
            boolean isTableWriteFragment = searchFrom(planFragment.getRoot())
                    .where(TableWriterNode.class::isInstance)
                    .findFirst().isPresent();
            if (planFragment.getId().getId() != 0 && !isTableWriteFragment) {
                PartitioningScheme outputPartitioning = planFragment.getPartitioningScheme();
                int outputPartitionCount = -1;
                if (outputPartitioning.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION)
                        || (outputPartitioning.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION))) {
                    outputPartitionCount = 1;
                }
                else if (outputPartitioning.getBucketToPartition().isPresent()) {
                    outputPartitionCount = outputPartitioning.getBucketToPartition().get().length;
                }

                if (outputPartitionCount != -1) {
                    task.setShuffleWriteInfo(exchangeProvider.generateExchangeWriteMetadata(
                            exchangeId,
                            taskIndexes.get(taskId)).toString());
                }
            }
        }
        return task;
    }

    @Override
    public String toString()
    {
        return stateMachine.toString();
    }

    public static ExecutionFailureInfo rewriteTransportFailure(ExecutionFailureInfo executionFailureInfo)
    {
        if (executionFailureInfo.getRemoteHost() == null) {
            return executionFailureInfo;
        }

        return new ExecutionFailureInfo(
                executionFailureInfo.getType(),
                executionFailureInfo.getMessage(),
                executionFailureInfo.getCause(),
                executionFailureInfo.getSuppressed(),
                executionFailureInfo.getStack(),
                executionFailureInfo.getErrorLocation(),
                REMOTE_HOST_GONE.toErrorCode(),
                executionFailureInfo.getRemoteHost(),
                executionFailureInfo.getErrorCause());
    }

    private SplitAssigner createSplitAssigner(
            Session session,
            PlanNodeId tableScanNodeId,
            SplitSource splitSource,
            PartitioningHandle fragmentPartitioning)
    {
        // TODO (MRScheduler) (fix fake shuffleMapProperties)
        ShuffleMapProperties shuffleMapProperties = new ShuffleMapProperties();
        shuffleMapProperties.setSplitAssignmentBatchSize(1000);
        shuffleMapProperties.setMaxSplitsDataSizePerPartition(new DataSize(1000, MEGABYTE));
        shuffleMapProperties.setPartitionCountAutoTuneEnabled(false);
        shuffleMapProperties.setInitialPartitionCount(105);
        shuffleMapProperties.setMinInputPartitionCountForAutoTune(10);
        shuffleMapProperties.setMaxSparkInputPartitionCountForAutoTune(1000);
        // splits from unbucketed table
        if (fragmentPartitioning.equals(SOURCE_DISTRIBUTION)) {
            return UnpartitionedSourceSplitAssigner.create(session, tableScanNodeId, splitSource, shuffleMapProperties);
        }
        // splits from bucketed table
        return PartitionedSourceSplitAssigner.create(session, tableScanNodeId, splitSource, fragmentPartitioning, partitioningProviderManager, shuffleMapProperties);
    }

    public interface SplitAssigner
            extends Closeable
    {
        Optional<SetMultimap<Integer, ScheduledSplit>> getNextBatch();
        void close();
    }

    public static class UnpartitionedSourceSplitAssigner
            implements SplitAssigner
    {
        private final PlanNodeId tableScanNodeId;
        private final SplitSource splitSource;

        private final int maxBatchSize;
        private final int initialPartitionCount;
        private final PriorityQueue<Partition> queue = new PriorityQueue<>();
        private int sequenceId;

        public static UnpartitionedSourceSplitAssigner create(
                Session session, PlanNodeId tableScanNodeId, SplitSource splitSource,
                ShuffleMapProperties shuffleMapProperties)
        {
            return new UnpartitionedSourceSplitAssigner(
                    tableScanNodeId,
                    splitSource,
                    shuffleMapProperties.getSplitAssignmentBatchSize(),
                    shuffleMapProperties.getMaxSplitsDataSizePerPartition().toBytes(),
                    shuffleMapProperties.getInitialPartitionCount(),
                    shuffleMapProperties.getMinInputPartitionCountForAutoTune(),
                    shuffleMapProperties.getMaxSparkInputPartitionCountForAutoTune());
        }

        public UnpartitionedSourceSplitAssigner(
                PlanNodeId tableScanNodeId,
                SplitSource splitSource,
                int maxBatchSize,
                long maxSplitsSizePerPartitionInBytes,
                int initialPartitionCount,
                int minSparkInputPartitionCountForAutoTune,
                int maxSparkInputPartitionCountForAutoTune)
        {
            this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.maxBatchSize = maxBatchSize;
            checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than zero");
            checkArgument(maxSplitsSizePerPartitionInBytes > 0,
                    "maxSplitsSizePerPartitionInBytes must be greater than zero: %s", maxSplitsSizePerPartitionInBytes);
            this.initialPartitionCount = initialPartitionCount;
            checkArgument(initialPartitionCount > 0,
                    "initialPartitionCount must be greater then zero: %s", initialPartitionCount);
            checkArgument(minSparkInputPartitionCountForAutoTune >= 1 && minSparkInputPartitionCountForAutoTune <= maxSparkInputPartitionCountForAutoTune,
                    "Min partition count for auto tune (%s) should be a positive integer and not larger than max partition count (%s)",
                    minSparkInputPartitionCountForAutoTune,
                    maxSparkInputPartitionCountForAutoTune);
        }

        @Override
        public Optional<SetMultimap<Integer, ScheduledSplit>> getNextBatch()
        {
            if (splitSource.isFinished()) {
                return Optional.empty();
            }

            List<ScheduledSplit> scheduledSplits = new ArrayList<>();
            while (true) {
                int remaining = maxBatchSize - scheduledSplits.size();
                if (remaining <= 0) {
                    break;
                }
                SplitSource.SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), min(remaining, 1000)));
                for (Split split : splitBatch.getSplits()) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScanNodeId, split));
                }
                if (splitBatch.isLastBatch() || splitSource.isFinished()) {
                    break;
                }
            }

            return Optional.of(assignSplitsToPartitions(scheduledSplits));
        }

        private SetMultimap<Integer, ScheduledSplit> assignSplitsToPartitions(List<ScheduledSplit> splits)
        {
            // expected to be mutable for efficiency reasons
            HashMultimap<Integer, ScheduledSplit> result = HashMultimap.create();
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                result.put(splitIndex % initialPartitionCount, splits.get(splitIndex));
            }
            return result;
        }

        @Override
        public void close()
        {
            splitSource.close();
        }

        private static class Partition
                implements Comparable<Partition>
        {
            private final int partitionId;
            private long splitsInBytes;

            public Partition(int partitionId)
            {
                this.partitionId = partitionId;
            }

            public int getPartitionId()
            {
                return partitionId;
            }

            public void assignSplitWithSize(long splitSizeInBytes)
            {
                splitsInBytes += splitSizeInBytes;
            }

            public long getSplitsInBytes()
            {
                return splitsInBytes;
            }

            @Override
            public int compareTo(Partition o)
            {
                // always prefer partition with a lower partition id to make split assignment result deterministic
                return ComparisonChain.start()
                        .compare(splitsInBytes, o.splitsInBytes)
                        .compare(partitionId, o.partitionId)
                        .result();
            }
        }
    }

    public static class PartitionedSourceSplitAssigner
            implements SplitAssigner
    {
        private final PlanNodeId tableScanNodeId;
        private final SplitSource splitSource;
        private final ToIntFunction<ConnectorSplit> splitBucketFunction;

        private final int maxBatchSize;

        private int sequenceId;

        public static PartitionedSourceSplitAssigner create(
                Session session,
                PlanNodeId tableScanNodeId,
                SplitSource splitSource,
                PartitioningHandle fragmentPartitioning,
                PartitioningProviderManager partitioningProviderManager,
                ShuffleMapProperties shuffleMapProperties)
        {
            return new PartitionedSourceSplitAssigner(
                    tableScanNodeId,
                    splitSource,
                    getSplitBucketFunction(session, fragmentPartitioning, partitioningProviderManager),
                    shuffleMapProperties.getSplitAssignmentBatchSize()); // TODO (MRScheduler) Make real properties
        }

        private static ToIntFunction<ConnectorSplit> getSplitBucketFunction(
                Session session,
                PartitioningHandle partitioning,
                PartitioningProviderManager partitioningProviderManager)
        {
            ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioning, partitioningProviderManager);
            return partitioningProvider.getSplitBucketFunction(
                    partitioning.getTransactionHandle().orElse(null),
                    session.toConnectorSession(),
                    partitioning.getConnectorHandle());
        }

        private static ConnectorNodePartitioningProvider getPartitioningProvider(PartitioningHandle partitioning, PartitioningProviderManager partitioningProviderManager)
        {
            ConnectorId connectorId = partitioning.getConnectorId()
                    .orElseThrow(() -> new IllegalArgumentException("Unexpected partitioning: " + partitioning));
            return partitioningProviderManager.getPartitioningProvider(connectorId);
        }

        public PartitionedSourceSplitAssigner(
                PlanNodeId tableScanNodeId,
                SplitSource splitSource,
                ToIntFunction<ConnectorSplit> splitBucketFunction,
                int maxBatchSize)
        {
            this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.splitBucketFunction = requireNonNull(splitBucketFunction, "splitBucketFunction is null");
            this.maxBatchSize = maxBatchSize;
            checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than zero");
        }

        @Override
        public Optional<SetMultimap<Integer, ScheduledSplit>> getNextBatch()
        {
            if (splitSource.isFinished()) {
                return Optional.empty();
            }

            List<ScheduledSplit> scheduledSplits = new ArrayList<>();
            while (true) {
                int remaining = maxBatchSize - scheduledSplits.size();
                if (remaining <= 0) {
                    break;
                }
                SplitSource.SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), min(remaining, 1000)));
                for (Split split : splitBatch.getSplits()) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScanNodeId, split));
                }
                if (splitBatch.isLastBatch() || splitSource.isFinished()) {
                    break;
                }
            }

            return Optional.of(assignSplitsToTasks(scheduledSplits));
        }

        private SetMultimap<Integer, ScheduledSplit> assignSplitsToTasks(List<ScheduledSplit> splits)
        {
            // expected to be mutable for efficiency reasons
            HashMultimap<Integer, ScheduledSplit> result = HashMultimap.create();
            for (ScheduledSplit scheduledSplit : splits) {
                int partitionId = splitBucketFunction.applyAsInt(scheduledSplit.getSplit().getConnectorSplit());
                result.put(partitionId, scheduledSplit);
            }
            return result;
        }

        @Override
        public void close()
        {
            splitSource.close();
        }
    }

    public static class ShuffleMapProperties
    {
        int splitAssignmentBatchSize;
        DataSize maxSplitsDataSizePerPartition = new DataSize(0, BYTE);
        int initialPartitionCount;
        boolean isPartitionCountAutoTuneEnabled;

        public int getSplitAssignmentBatchSize()
        {
            return splitAssignmentBatchSize;
        }

        public void setSplitAssignmentBatchSize(int splitAssignmentBatchSize)
        {
            this.splitAssignmentBatchSize = splitAssignmentBatchSize;
        }

        public DataSize getMaxSplitsDataSizePerPartition()
        {
            return maxSplitsDataSizePerPartition;
        }

        public void setMaxSplitsDataSizePerPartition(DataSize maxSplitsDataSizePerPartition)
        {
            this.maxSplitsDataSizePerPartition = maxSplitsDataSizePerPartition;
        }

        public int getInitialPartitionCount()
        {
            return initialPartitionCount;
        }

        public void setInitialPartitionCount(int initialPartitionCount)
        {
            this.initialPartitionCount = initialPartitionCount;
        }

        public boolean isPartitionCountAutoTuneEnabled()
        {
            return isPartitionCountAutoTuneEnabled;
        }

        public void setPartitionCountAutoTuneEnabled(boolean partitionCountAutoTuneEnabled)
        {
            isPartitionCountAutoTuneEnabled = partitionCountAutoTuneEnabled;
        }

        public int getMinInputPartitionCountForAutoTune()
        {
            return minInputPartitionCountForAutoTune;
        }

        public void setMinInputPartitionCountForAutoTune(int minInputPartitionCountForAutoTune)
        {
            this.minInputPartitionCountForAutoTune = minInputPartitionCountForAutoTune;
        }

        public int getMaxSparkInputPartitionCountForAutoTune()
        {
            return maxSparkInputPartitionCountForAutoTune;
        }

        public void setMaxSparkInputPartitionCountForAutoTune(int maxSparkInputPartitionCountForAutoTune)
        {
            this.maxSparkInputPartitionCountForAutoTune = maxSparkInputPartitionCountForAutoTune;
        }

        int minInputPartitionCountForAutoTune;
        int maxSparkInputPartitionCountForAutoTune;
    }
}
