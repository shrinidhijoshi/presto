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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkShuffleInfoTranslator;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;

/**
 * PrestoSparkNativeTaskExecution represents the Native Execution of a Task received from the driver/coordinator to the executor.
 * It is a thin shim used that delegates all the Task execution details to the CPP process. This is done by forwarding the serialized data from {@link PrestoSparkTaskDescriptor}
 * to the NativeExecution CPP process
 * As a part of the first task execution, it also triggers the creation of the CPP process, through the {@link NativeExecutionProcessFactory} get() call
 *
 * Further, it returns a NativeExecutionTaskOutput which is a dummy output to be processed by the RDD/Shuffle layer. This is because, when the
 * task execution happens on the CPP process, the shuffle/table read as well as the shuffle write too happens on the CPP process.
 * We still return a dummy(null) output from here as we want to keep the integration at the Shuffle/RDD layer opaque to native execution.
 * Further note that this dummy output is consumed by reader and writer defined in {@link com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager}
 *
 * Another important function of this class is to forward the {@link TaskInfo} and {@link ShuffleStats} through the collector API
 * to the driver so that task and shuffle metrics for the query can be processed correctly.
 */
public class PrestoSparkNativeTaskExecution
{
    private static final Logger log = Logger.get(PrestoSparkNativeTaskExecution.class);
    private static final String SHUFFLE_READ_INFO_LOCATION_TEMPLATE = "batch://%s?shuffleInfo=%s";
    private final Session session;
    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<TaskSource> taskSourceCodec;
    private final PrestoSparkTaskDescriptor prestoSparkTaskDescriptor;
    private final NativeExecutionProcessFactory nativeExecutionProcessFactory;
    private final NativeExecutionTaskFactory nativeExecutionTaskFactory;
    private final PrestoSparkShuffleInfoTranslator prestoSparkShuffleInfoTranslator;
    private final PrestoSparkNativeTaskInputs prestoSparkNativeTaskInputs;
    private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
    private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
    private final Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources;

    public PrestoSparkNativeTaskExecution(
            Session session,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskSource> taskSourceCodec,
            PrestoSparkTaskDescriptor prestoSparkTaskDescriptor,
            PrestoSparkNativeTaskInputs prestoSparkNativeTaskInputs,
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            NativeExecutionProcessFactory nativeExecutionProcessFactory,
            NativeExecutionTaskFactory nativeExecutionTaskFactory,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            PrestoSparkShuffleInfoTranslator prestoSparkShuffleInfoTranslator)
    {
        this.session = session;
        this.prestoSparkTaskDescriptor = prestoSparkTaskDescriptor;
        this.nativeExecutionProcessFactory = nativeExecutionProcessFactory;
        this.nativeExecutionTaskFactory = nativeExecutionTaskFactory;
        this.prestoSparkShuffleInfoTranslator = prestoSparkShuffleInfoTranslator;
        this.prestoSparkNativeTaskInputs = prestoSparkNativeTaskInputs;
        this.taskInfoCollector = taskInfoCollector;
        this.shuffleStatsCollector = shuffleStatsCollector;
        this.serializedTaskSources = serializedTaskSources;
        this.taskInfoCodec = taskInfoCodec;
        this.taskSourceCodec = taskSourceCodec;
    }

    public NativeExecutionEmptyOutput<?> start(TaskId taskId)
    {
        PlanFragment fragment = prestoSparkTaskDescriptor.getFragment();

        ImmutableMap<PlanNodeId, PrestoSparkShuffleReadInfo> shuffleReadInfos = getShuffleReadInfos(fragment, prestoSparkNativeTaskInputs);
        Optional<PrestoSparkShuffleWriteInfo> shuffleWriteInfo = prestoSparkNativeTaskInputs.getShuffleWriteDescriptor().isPresent() && !findTableWriteNode(fragment.getRoot()).isPresent() ?
                Optional.of(prestoSparkShuffleInfoTranslator.createShuffleWriteInfo(session, prestoSparkNativeTaskInputs.getShuffleWriteDescriptor().get())) : Optional.empty();
        List<TaskSource> taskSources = getNativeExecutionShuffleSources(taskId, fragment, shuffleReadInfos, getTaskSources(serializedTaskSources));

        // This will create the native process if one doesn't exist
        NativeExecutionProcess nativeExecutionProcess = nativeExecutionProcessFactory.createNativeExecutionProcess(
                session,
                URI.create("http://127.0.0.1/"));

        log.info("Submitting native execution task ");
        NativeExecutionTask task = nativeExecutionTaskFactory.createNativeExecutionTask(
                session,
                nativeExecutionProcess.getLocation(),
                taskId,
                fragment,
                taskSources,
                prestoSparkTaskDescriptor.getTableWriteInfo(),
                shuffleWriteInfo.map(prestoSparkShuffleInfoTranslator::createSerializedWriteInfo));
        TaskInfo taskInfo = task.start();

        return new NativeExecutionEmptyOutput<>(task, taskInfoCollector, shuffleStatsCollector, taskInfoCodec);
    }

    private ImmutableMap<PlanNodeId, PrestoSparkShuffleReadInfo> getShuffleReadInfos(
            PlanFragment fragment,
            PrestoSparkNativeTaskInputs inputs)
    {
        ImmutableMap.Builder<PlanNodeId, PrestoSparkShuffleReadInfo> shuffleReadInfos = ImmutableMap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                PrestoSparkShuffleReadDescriptor shuffleReadDescriptor = inputs.getShuffleReadDescriptors().get(sourceFragmentId.toString());
                if (shuffleReadDescriptor != null) {
                    shuffleReadInfos.put(remoteSource.getId(), prestoSparkShuffleInfoTranslator.createShuffleReadInfo(session, shuffleReadDescriptor));
                }
            }
        }
        return shuffleReadInfos.build();
    }

    /**
     * This function combines the shuffle sources and task sources to generate the final list of
     * sources to be passed to the CPP process
     * The main operation here is that the splits from the shuffle source are merged into the splits from
     * the root-node source splits. //TODO : Explanation of why this is done
     *
     * @param taskId - id of the task
     * @param fragment - fragment to execute
     * @param shuffleReadInfos - this contains the shuffleReadInfo for each PlanNode
     * @param taskSources - list of sources which define the input
     * @return List of task sources
     */
    private List<TaskSource> getNativeExecutionShuffleSources(TaskId taskId, PlanFragment fragment, Map<PlanNodeId, PrestoSparkShuffleReadInfo> shuffleReadInfos, List<TaskSource> taskSources)
    {
        ImmutableSet.Builder<ScheduledSplit> result = ImmutableSet.builder();
        PlanNode root = fragment.getRoot();
        AtomicLong nextSplitId = new AtomicLong();

        // Shuffle splits
        shuffleReadInfos.forEach((planNodeId, info) ->
                result.add(new ScheduledSplit(
                        nextSplitId.getAndIncrement(),
                        planNodeId,
                        new Split(
                                REMOTE_CONNECTOR_ID,
                                new RemoteTransactionHandle(),
                                new RemoteSplit(
                                    new Location(format(SHUFFLE_READ_INFO_LOCATION_TEMPLATE, taskId, prestoSparkShuffleInfoTranslator.createSerializedReadInfo(info))),
                                    taskId)))));

        // Source splits for root node
        List<TaskSource> rootPlanNodeSources = taskSources.stream().filter(taskSource -> taskSource.getPlanNodeId().equals(root.getId())).collect(Collectors.toList());
        checkState(rootPlanNodeSources.size() <= 1, "At most 1 taskSource is expected for NativeExecutionNode but got %s", rootPlanNodeSources.size());
        if (!rootPlanNodeSources.isEmpty()) {
            TaskSource rootPlanNodeSource = rootPlanNodeSources.get(0);
            result.addAll(rootPlanNodeSource.getSplits());
        }

        // create a new taskSource with the shuffleSplits and rootNodeSplits
        TaskSource newTaskSource = new TaskSource(root.getId(), result.build(), ImmutableSet.of(Lifespan.taskWide()), true);

        ImmutableList.Builder<TaskSource> newTaskSources = ImmutableList.builder();
        // Combine the shuffle read taskSource and original sources
        newTaskSources.add(newTaskSource)
                .addAll(taskSources.stream()
                        .filter(taskSource -> !taskSource.getPlanNodeId().equals(root.getId()))
                        .collect(Collectors.toList()));

        return newTaskSources.build();
    }

    /**
     * This class has 2 things to take care of
     *
     * 1) Is to fulfill the RDD contract of returning Iterator<Tuple2<MutablePartitionId, T>>
     * as the result of task execution.
     * Note that for a native task execution, there is nothing to return to the shuffle layer. Hence we return null.
     * Also, the shuffle layer expects that in fact nothing is returned
     *
     * 2) Send task stats and shuffle stats to driver by updating the taskInfoCollector and shuffleStatsCollector
     * Collector APIs
     *
     * @param <T>
     */
    private static class NativeExecutionEmptyOutput<T extends PrestoSparkTaskOutput>
            extends AbstractIterator<Tuple2<MutablePartitionId, T>>
            implements IPrestoSparkTaskExecutor<T>
    {
        private final NativeExecutionTask nativeExecutionTask;
        private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
        private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
        private final Codec<TaskInfo> taskInfoCodec;
        private boolean completed;
        public NativeExecutionEmptyOutput(
                NativeExecutionTask nativeExecutionTask,
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
                CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
                Codec<TaskInfo> taskInfoCodec)
        {
            this.nativeExecutionTask = nativeExecutionTask;
            this.taskInfoCollector = taskInfoCollector;
            this.shuffleStatsCollector = shuffleStatsCollector;
            this.taskInfoCodec = taskInfoCodec;
        }
        @Override
        public boolean hasNext()
        {
            return !completed;
        }

        @Override
        public Tuple2<MutablePartitionId, T> next()
        {
            log.info("NativeExecutionOutput.next called");
            completed = true;
            try {
                Optional<SerializedPage> page = nativeExecutionTask.pollResult();
                log.info("nativeExecutionTask.pollResult() = %s", page);

//                SerializedTaskInfo serializedTaskInfo = new SerializedTaskInfo(serializeZstdCompressed(taskInfoCodec, taskInfo));
//                taskInfoCollector.add(serializedTaskInfo);
                log.info("NativeExecutionOutput.next returning null");
                return new Tuple2<>(new MutablePartitionId(), null);
            }
            catch (InterruptedException e) {
                log.error("NativeExecutionOutput.next ", e);
                throw new RuntimeException(e);
            }
        }
    }

    private Optional<TableWriterNode> findTableWriteNode(PlanNode node)
    {
        return searchFrom(node)
                .where(TableWriterNode.class::isInstance)
                .findFirst();
    }

    private List<TaskSource> getTaskSources(Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources)
    {
        long totalSerializedSizeInBytes = 0;
        ImmutableList.Builder<TaskSource> result = ImmutableList.builder();
        while (serializedTaskSources.hasNext()) {
            SerializedPrestoSparkTaskSource serializedTaskSource = serializedTaskSources.next();
            totalSerializedSizeInBytes += serializedTaskSource.getBytes().length;
            result.add(deserializeZstdCompressed(taskSourceCodec, serializedTaskSource.getBytes()));
        }
        log.info("Total serialized size of all task sources: %s", succinctBytes(totalSerializedSizeInBytes));
        return result.build();
    }
}
