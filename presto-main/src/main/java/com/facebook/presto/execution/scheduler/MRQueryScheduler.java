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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.mapreduce.MRStageExecution;
import com.facebook.presto.execution.scheduler.mapreduce.MRTableCommitMetadataCache;
import com.facebook.presto.execution.scheduler.mapreduce.MRTaskScheduler;
import com.facebook.presto.execution.scheduler.mapreduce.exchange.ExchangeDependency;
import com.facebook.presto.execution.scheduler.mapreduce.exchange.ExchangeProviderRegistry;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.isRuntimeOptimizerEnabled;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.StageExecutionState.ABORTED;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.PLANNED;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.execution.scheduler.mapreduce.MRStageExecution.createStageExecution;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;

public class MRQueryScheduler
        implements SqlQuerySchedulerInterface
{
    private static final Logger log = Logger.get(MRQueryScheduler.class);

    private final LocationFactory locationFactory;
    private final SplitSchedulerStats schedulerStats;
    private final QueryStateMachine queryStateMachine;
    private final AtomicReference<SubPlan> plan = new AtomicReference<>();
    private final StageId rootStageId;
    private final boolean summarizeTaskInfo;
    // The following fields are required by adaptive optimization in runtime.
    private final Session session;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final List<PlanOptimizer> runtimePlanOptimizers;
    private final WarningCollector warningCollector;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final Set<StageId> runtimeOptimizedStages = Collections.synchronizedSet(new HashSet<>());
    private final PlanChecker planChecker;
    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final Map<StageId, MRStageExecution> stageExecutions = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final PartitioningProviderManager partitioningProviderManager;
    private final MRTaskScheduler mrTaskScheduler;
    private final Set<MRStageExecution> currentlyRunningStageExecutions;
    private final TableWriteInfo tableWriteInfo;
    private final MRTableCommitMetadataCache mrTableCommitMetadataCache;
    private final ExchangeProviderRegistry exchangeProviderRegistry;

    public static MRQueryScheduler createSqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutorService queryExecutor,
            SplitSchedulerStats schedulerStats,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            FunctionAndTypeManager functionAndTypeManager,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser,
            PartitioningProviderManager partitioningProviderManager,
            MRTaskScheduler mrTaskScheduler,
            MRTableCommitMetadataCache mrTableCommitMetadataCache,
            ExchangeProviderRegistry exchangeProviderRegistry)
    {
        MRQueryScheduler sqlQueryScheduler = new MRQueryScheduler(
                locationFactory,
                queryExecutor,
                schedulerStats,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                functionAndTypeManager,
                queryStateMachine,
                plan,
                summarizeTaskInfo,
                runtimePlanOptimizers,
                warningCollector,
                idAllocator,
                variableAllocator,
                planChecker,
                metadata,
                sqlParser,
                partitioningProviderManager,
                mrTaskScheduler,
                mrTableCommitMetadataCache,
                exchangeProviderRegistry);
        try {
            sqlQueryScheduler.initialize();
        }
        catch (Exception ex) {
            log.error("Error initializing query", ex);
        }
        return sqlQueryScheduler;
    }

    private MRQueryScheduler(
            LocationFactory locationFactory,
            ExecutorService queryExecutor,
            SplitSchedulerStats schedulerStats,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            FunctionAndTypeManager functionAndTypeManager,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser,
            PartitioningProviderManager partitioningProviderManager,
            MRTaskScheduler mrTaskScheduler,
            MRTableCommitMetadataCache mrTableCommitMetadataCache,
            ExchangeProviderRegistry exchangeProviderRegistry)
    {
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executor = queryExecutor;
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.session = requireNonNull(session, "session is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.runtimePlanOptimizers = requireNonNull(runtimePlanOptimizers, "runtimePlanOptimizers is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.partitioningProviderManager = partitioningProviderManager;
        this.mrTaskScheduler = mrTaskScheduler;
        this.mrTableCommitMetadataCache = mrTableCommitMetadataCache;
        this.exchangeProviderRegistry = exchangeProviderRegistry;
        this.summarizeTaskInfo = summarizeTaskInfo;
        currentlyRunningStageExecutions = new HashSet<>();

        // configure output partitioning
        plan = configureOutputPartitioning(session, plan, getHashPartitionCount(session));

        // compute table write info
        StreamingPlanSection streamingPlanSection = extractStreamingSections(plan);
        StreamingSubPlan streamingSubPlan = streamingPlanSection.getPlan();
        tableWriteInfo = createTableWriteInfo(streamingSubPlan, metadata, session);
        this.plan.compareAndSet(null, requireNonNull(plan, "plan is null"));

        log.error(PlanPrinter.textDistributedPlan(plan, functionAndTypeManager, session, true));

        try {
            // Create stage executions DAG
            List<MRStageExecution> stageExecutions = createMRStageExecutions(
                    plan,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    exchangeProviderRegistry);
            this.rootStageId = Iterables.getLast(stageExecutions).getStageExecutionId().getStageId();

            stageExecutions.stream()
                    .forEach(stageExecution -> this.stageExecutions.put(stageExecution.getStageExecutionId().getStageId(), stageExecution));
        }
        catch (Exception ex) {
            log.error("Error building DAG: ", ex);
            throw ex;
        }
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        MRStageExecution rootStage = stageExecutions.get(rootStageId);
        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        addStateChangeListeners(new ArrayList<>(stageExecutions.values()));

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    public void start()
    {
        long delay = 2000;
        if (started.compareAndSet(false, true)) {
            requireNonNull(stageExecutions);
            // still scheduling the previous batch of stages
            if (scheduling.get()) {
                return;
            }
            executor.submit(() -> {
                if (!scheduling.compareAndSet(false, true)) {
                    // still scheduling the previous batch of stages
                    return;
                }

                try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                    while (!Thread.currentThread().isInterrupted()) {
                        // Step2
                        boolean continueExecution = refreshDAGAndQueueTasks();
                        if (!continueExecution) {
                            break;
                        }

                        // Step3
                        mrTaskScheduler.offerAvailableSlotsToStageExecutions();

                        log.info("SchedulerLoop.. 2sec sleep");
                        Thread.sleep(delay);
                    }

                    scheduling.set(false);
                }
                catch (Throwable ex) {
                    scheduling.set(false);
                    queryStateMachine.transitionToFailed(ex);
                    log.error("Error occurred during QueryExecution!", ex);
                    ex.printStackTrace();
                }
                finally {
                    RuntimeException closeError = new RuntimeException();
                    for (MRStageExecution stageExecution : currentlyRunningStageExecutions) {
                        try {
                            stageExecution.abort();
                        }
                        catch (Throwable t) {
                            queryStateMachine.transitionToFailed(t);
                            // Self-suppression not permitted
                            if (closeError != t) {
                                closeError.addSuppressed(t);
                            }
                        }
                    }
                    if (closeError.getSuppressed().length > 0) {
                        throw closeError;
                    }
                }
                log.info("----- ALL STAGE EXECUTION FINISHED ------- ");
                queryStateMachine.transitionToFinishing();
            });
        }
    }

    private boolean refreshDAGAndQueueTasks()
    {
        // refresh the stage state
        currentlyRunningStageExecutions.forEach(MRStageExecution::schedule);

        // Check if any finished stages completed
        Set<MRStageExecution> finishedStages = currentlyRunningStageExecutions.stream()
                .filter(stageExecution -> stageExecution.getState().isDone())
                .collect(Collectors.toSet());

        // Update stage tracking
        currentlyRunningStageExecutions.removeAll(finishedStages);
        StringBuilder stringBuilder = new StringBuilder();
        currentlyRunningStageExecutions.forEach(stageExecution -> {
            stringBuilder.append("[");
            stringBuilder.append(stageExecution.getFragment().getId());
            stringBuilder.append("]");
        });
        log.info("CURRENT STAGES = %s", stringBuilder.toString());
        if (finishedStages.isEmpty() && !currentlyRunningStageExecutions.isEmpty()) {
            log.info("None of currently running stages have finished.. 1sec sleep");
            return true;
        }

        // try to pull more section that are ready to be run
        List<SubPlan> newSectionsReadyForExecution = getSectionsReadyForExecution();

        // all finished
        if (newSectionsReadyForExecution.isEmpty() && currentlyRunningStageExecutions.isEmpty()) {
            log.info("No new stages ready for scheduling and all stages finished");
            return false;
        }

        List<MRStageExecution> newStageExecutionsToSchedule = getStageExecutionsForFragments(newSectionsReadyForExecution);

        // schedule new stage executions
        for (MRStageExecution stageExecution : newStageExecutionsToSchedule) {
            // track this stage as being executed
            currentlyRunningStageExecutions.add(stageExecution);
            stageExecution.schedule();
        }
        return true;
    }

    /**
     * returns a List of StageExecutionInfos in a postorder representation of the tree
     */
    private List<MRStageExecution> createMRStageExecutions(
            SubPlan section,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            ExchangeProviderRegistry exchangeProviderRegistry)
    {
        ImmutableList.Builder<MRStageExecution> stages = ImmutableList.builder();

        List<ExchangeDependency> exchangeDependencies = new ArrayList<>();
        for (SubPlan childSection : section.getChildren()) {
            List<MRStageExecution> childStageTree = createMRStageExecutions(
                    childSection,
                    remoteTaskFactory,
                    splitSourceFactory,
                    session,
                    exchangeProviderRegistry);
            stages.addAll(childStageTree);
            exchangeDependencies.add(childStageTree.get(childStageTree.size() - 1).getExchangeOutput());
        }

        stages.add(createMRStageExecution(
                session,
                section,
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                0,
                partitioningProviderManager,
                exchangeProviderRegistry,
                exchangeDependencies));

        return stages.build();
    }

    private List<SubPlan> getSectionsReadyForExecution()
    {
        return stream(forTree(SubPlan::getChildren).depthFirstPreOrder(plan.get()))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .map(this::tryCostBasedOptimize)
                .collect(toImmutableList());
    }

    /**
     * A general purpose utility function to invoke runtime cost-based optimizer.
     * (right now there is only one plan optimizer which determines if the probe and build side of a JoinNode should be swapped
     * based on the statistics of the temporary table holding materialized exchange outputs from finished children sections)
     */
    private SubPlan tryCostBasedOptimize(SubPlan subPlan)
    {
        // no need to do runtime optimization if no materialized exchange data is utilized by the section.
        if (!isRuntimeOptimizerEnabled(session) || subPlan.getChildren().isEmpty()) {
            return subPlan;
        }

        // Apply runtime optimization on each SubPlan and generate optimized new fragments
        Map<PlanFragment, PlanFragment> oldToNewFragment = new HashMap<>();
        stream(forTree(SubPlan::getChildren).depthFirstPreOrder(subPlan))
                .forEach(currentSubPlan -> {
                    Optional<PlanFragment> newPlanFragment = performRuntimeOptimizations(currentSubPlan);
                    if (newPlanFragment.isPresent()) {
                        planChecker.validatePlanFragment(newPlanFragment.get().getRoot(), session, metadata, sqlParser, TypeProvider.viewOf(variableAllocator.getVariables()), warningCollector);
                        oldToNewFragment.put(currentSubPlan.getFragment(), newPlanFragment.get());
                    }
                });

        // Early exit when no stage's fragment is changed
        if (oldToNewFragment.isEmpty()) {
            return subPlan;
        }

        oldToNewFragment.forEach((oldFragment, newFragment) -> runtimeOptimizedStages.add(getStageId(oldFragment.getId())));

        // Update SubPlan so that getStageInfo will reflect the latest optimized plan when query is finished.
        updatePlan(oldToNewFragment);

        // Rebuild and update entries of the stageExecutions map.
        updateStageExecutions(subPlan, oldToNewFragment);
        log.debug("Invoked CBO during runtime, optimized stage IDs: " + oldToNewFragment.keySet().stream()
                .map(PlanFragment::getId)
                .map(PlanFragmentId::toString)
                .collect(Collectors.joining(", ")));
        return subPlan;
    }

    private Optional<PlanFragment> performRuntimeOptimizations(SubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        PlanNode newRoot = fragment.getRoot();
        for (PlanOptimizer optimizer : runtimePlanOptimizers) {
            newRoot = optimizer.optimize(newRoot, session, TypeProvider.viewOf(variableAllocator.getVariables()), variableAllocator, idAllocator, warningCollector);
        }
        if (newRoot != fragment.getRoot()) {
            StatsAndCosts estimatedStatsAndCosts = fragment.getStatsAndCosts();
            return Optional.of(
                    // The partitioningScheme should stay the same
                    // even if the root's outputVariable layout is changed.
                    new PlanFragment(
                            fragment.getId(),
                            newRoot,
                            fragment.getVariables(),
                            fragment.getPartitioning(),
                            scheduleOrder(newRoot),
                            fragment.getPartitioningScheme(),
                            fragment.getStageExecutionDescriptor(),
                            fragment.isOutputTableWriterFragment(),
                            estimatedStatsAndCosts,
                            Optional.of(jsonFragmentPlan(newRoot, fragment.getVariables(), estimatedStatsAndCosts, functionAndTypeManager, session))));
        }
        return Optional.empty();
    }

    /**
     * Utility function that rebuild a SubPlan, re-create stageExecutionAndScheduler for each of its stage, and finally update the stageExecutions map.
     */
    private void updateStageExecutions(SubPlan subPlan, Map<PlanFragment, PlanFragment> oldToNewFragment)
    {
        SubPlan newSubPlan = rewriteSubPlan(subPlan, oldToNewFragment);
        List<MRStageExecution> newStageExecutions = createMRStageExecutions(
                newSubPlan,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                exchangeProviderRegistry);
        addStateChangeListeners(newStageExecutions);
        Map<StageId, MRStageExecution> updatedStageExecutions = newStageExecutions.stream()
                .collect(toImmutableMap(stageExecution -> stageExecution.getStageExecutionId().getStageId(), identity()));
        synchronized (this) {
            stageExecutions.putAll(updatedStageExecutions);
        }
    }

    private void updatePlan(Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        plan.getAndUpdate(value -> rewritePlan(value, oldToNewFragments));
    }

    private SubPlan rewritePlan(SubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        ImmutableList.Builder<SubPlan> children = ImmutableList.builder();
        for (SubPlan child : root.getChildren()) {
            children.add(rewritePlan(child, oldToNewFragments));
        }
        if (oldToNewFragments.containsKey(root.getFragment())) {
            return new SubPlan(oldToNewFragments.get(root.getFragment()), children.build());
        }
        else {
            return new SubPlan(root.getFragment(), children.build());
        }
    }

    // Only used for adaptive optimization, to register listeners to new stageExecutions generated in runtime.
    private void addStateChangeListeners(List<MRStageExecution> stageExecutions)
    {
        for (MRStageExecution stageExecution : stageExecutions) {
            if (isRootFragment(stageExecution.getFragment())) {
                stageExecution.addStateChangeListener(state -> {
                    if (state == FINISHED) {
                        queryStateMachine.transitionToFinishing();
                    }
                    else if (state == CANCELED) {
                        // output stage was canceled
                        queryStateMachine.transitionToCanceled();
                    }
                });
            }
            stageExecution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stageExecution.getStageExecutionInfo().getFailureCause().get().toException());
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    // startScheduling();
                    log.info("Nothing to do");
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stageExecution.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
            stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }
    }

    private SubPlan rewriteSubPlan(SubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragment)
    {
        ImmutableList.Builder<SubPlan> childrenPlans = ImmutableList.builder();
        for (SubPlan child : root.getChildren()) {
            childrenPlans.add(rewriteSubPlan(child, oldToNewFragment));
        }
        if (oldToNewFragment.containsKey(root.getFragment())) {
            return new SubPlan(oldToNewFragment.get(root.getFragment()), childrenPlans.build());
        }
        else {
            return new SubPlan(root.getFragment(), childrenPlans.build());
        }
    }

    private static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    private boolean isReadyForExecution(SubPlan plan)
    {
        MRStageExecution stageExecution = getStageExecution(plan.getFragment().getId());
        if (stageExecution.getState() != PLANNED) {
            // already scheduled
            return false;
        }
        for (SubPlan child : plan.getChildren()) {
            MRStageExecution rootStageExecution = getStageExecution(child.getFragment().getId());
            if (rootStageExecution.getState() != FINISHED) {
                return false;
            }
        }
        return true;
    }

    private List<MRStageExecution> getStageExecutionsForFragments(List<SubPlan> subPlans)
    {
        return subPlans.stream()
            .map(SubPlan::getFragment)
            .map(PlanFragment::getId)
            .map(this::getStageExecution)
            .collect(toImmutableList());
    }

    private MRStageExecution getStageExecution(PlanFragmentId planFragmentId)
    {
        return stageExecutions.get(getStageId(planFragmentId));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(queryStateMachine.getQueryId(), fragmentId.getId());
    }

    public long getUserMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecution -> stageExecution.getUserMemoryReservation())
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecution -> stageExecution.getTotalMemoryReservation())
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stageExecutions.values().stream()
                .mapToLong(stageExecution -> stageExecution.getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    @Override
    public DataSize getRawInputDataSize()
    {
        long datasize = stageExecutions.values().stream()
                .mapToLong(stageExecution -> stageExecution.getRawInputDataSize().toBytes())
                .sum();
        return DataSize.succinctBytes(datasize);
    }

    @Override
    public long getOutputPositions()
    {
        return stageExecutions.get(rootStageId).getStageExecutionInfo().getStats().getOutputPositions();
    }

    @Override
    public DataSize getOutputDataSize()
    {
        return stageExecutions.get(rootStageId).getStageExecutionInfo().getStats().getOutputDataSize();
    }

    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = stageExecutions.values().stream()
                .map(stageExecution -> stageExecution.getBasicStageStats())
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageExecutionInfo> stageInfos = stageExecutions.values().stream()
                .collect(toImmutableMap(execution -> execution.getStageExecutionId().getStageId(), MRStageExecution::getStageExecutionInfo));

        return buildStageInfo(plan.get(), stageInfos);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, Map<StageId, StageExecutionInfo> stageExecutionInfos)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        StageExecutionInfo stageExecutionInfo = stageExecutionInfos.get(stageId);
        checkArgument(stageExecutionInfo != null, "No stageExecutionInfo for %s", stageId);
        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                stageExecutionInfo,
                ImmutableList.of(),
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutionInfos))
                        .collect(toImmutableList()),
                runtimeOptimizedStages.contains(stageId));
    }

    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            MRStageExecution execution = stageExecutions.get(stageId);
            MRStageExecution stage = requireNonNull(execution, () -> format("Stage %s does not exist", stageId));
            stage.cancel();
        }
    }

    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stageExecutions.values().forEach(MRStageExecution::abort);
        }
    }

    protected SubPlan configureOutputPartitioning(Session session, SubPlan subPlan, int hashPartitionCount)
    {
        PlanFragment fragment = subPlan.getFragment();
        if (!fragment.getPartitioningScheme().getBucketToPartition().isPresent()) {
            PartitioningHandle partitioningHandle = fragment.getPartitioningScheme().getPartitioning().getHandle();

            // fix bucketToPartition
            Optional<int[]> bucketToPartition = Optional.empty();

            if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                bucketToPartition = Optional.of(IntStream.range(0, hashPartitionCount).toArray());
            }
            //  FIXED_ARBITRARY_DISTRIBUTION is used for UNION ALL
            //  UNION ALL inputs could be source inputs or shuffle inputs
            if (partitioningHandle.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
                // given modular hash function, partition count could be arbitrary size
                // simply reuse hash_partition_count for convenience
                // it can also be set by a separate session property if needed
                bucketToPartition = Optional.of(IntStream.range(0, hashPartitionCount).toArray());
            }
            if (partitioningHandle.getConnectorId().isPresent()) {
                ConnectorId connectorId = partitioningHandle.getConnectorId()
                        .orElseThrow(() -> new IllegalArgumentException("Unexpected partitioning: " + partitioningHandle));
                int connectorPartitionCount = partitioningProviderManager.getPartitioningProvider(connectorId).getBucketCount(
                        partitioningHandle.getTransactionHandle().orElse(null),
                        session.toConnectorSession(),
                        partitioningHandle.getConnectorHandle());
                bucketToPartition = Optional.of(IntStream.range(0, connectorPartitionCount).toArray());
            }

            if (bucketToPartition.isPresent()) {
                fragment = fragment.withBucketToPartition(bucketToPartition);
            }
        }
        return new SubPlan(
                fragment,
                subPlan.getChildren().stream()
                        .map(child -> configureOutputPartitioning(session, child, hashPartitionCount))
                        .collect(toImmutableList()));
    }

    public MRStageExecution createMRStageExecution(
            Session session,
            SubPlan subPlan,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId,
            PartitioningProviderManager partitioningProviderManager,
            ExchangeProviderRegistry exchangeProviderRegistry,
            List<ExchangeDependency> exchangeInputs)
    {
        PlanFragmentId fragmentId = subPlan.getFragment().getId();
        StageId stageId = new StageId(session.getQueryId(), fragmentId.getId());

        ExchangeLocationsConsumer locationsConsumer;
        OutputBuffers outputBuffers;
        if (subPlan.getFragment().getId().getId() == 0) {
            outputBuffers = createInitialEmptyOutputBuffers(subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle())
                    .withBuffer(new OutputBuffers.OutputBufferId(0), BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds();
            OutputBuffers.OutputBufferId rootBufferId = getOnlyElement(outputBuffers.getBuffers().keySet());
            locationsConsumer = (planFragmentId, tasks, noMoreExchangeLocations) ->
                    updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
        }
        else {
            outputBuffers = createDiscardingOutputBuffers();
            locationsConsumer = (planFragmentId, tasks, noMoreExchangeLocations) -> {};
        }

        return createStageExecution(
                new StageExecutionId(stageId, attemptId),
                subPlan.getFragment(),
                remoteTaskFactory,
                session,
                outputBuffers,
                locationsConsumer,
                summarizeTaskInfo,
                executor,
                schedulerStats,
                tableWriteInfo,
                partitioningProviderManager,
                splitSourceFactory,
                mrTaskScheduler,
                mrTableCommitMetadataCache,
                exchangeProviderRegistry,
                exchangeInputs);
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBuffers.OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBuffers.OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }
}
