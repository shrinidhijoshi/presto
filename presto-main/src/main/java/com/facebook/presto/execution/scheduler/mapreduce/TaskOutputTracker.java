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
import com.facebook.presto.execution.TaskInfo;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TaskOutputTracker is used by a given stage execution to track which
 * attempt of each of its task was finally considered successfull.
 *
 * This is to combat multiple attempts of the same task writing to shuffle
 * but only 1 of those acks reaching the coordinator.
 */
public class TaskOutputTracker
{
    private static Logger logger = Logger.get(TaskOutputTracker.class);
    private final AtomicReferenceArray<TaskInfo> successTaskIds;
    private final Object[] mapOutputMetadata;
    private long committedTaskCount;
    private final Function<TaskInfo, Object> metadataGenerator;
    public TaskOutputTracker(int taskCount, Function<TaskInfo, Object> metadataGenerator)
    {
        successTaskIds = new AtomicReferenceArray<>(taskCount);
        mapOutputMetadata = new Object[taskCount];
        this.metadataGenerator = metadataGenerator;
    }

    public boolean tryCommit(TaskInfo taskInfo)
    {
        if (!successTaskIds.compareAndSet(taskInfo.getTaskId().getId(), null, taskInfo)) {
            logger.error("Failed to commit task %s attempt %s, as another attempt %s is already committed", taskInfo.getTaskId().getId(), taskInfo.getTaskId().getAttemptNumber(), successTaskIds.get(taskInfo.getTaskId().getId()).getTaskId().getAttemptNumber());
            return false;
        }
        committedTaskCount++;
        mapOutputMetadata[taskInfo.getTaskId().getId()] = metadataGenerator.apply(taskInfo);
        return true;
    }

    public boolean isAllCommitted()
    {
        return committedTaskCount == successTaskIds.length();
    }

    public List<TaskInfo> getAllTaskInfos()
    {
        return IntStream.range(0, successTaskIds.length())
                .mapToObj(successTaskIds::get)
                .collect(Collectors.toList());
    }

    public Object getMetadataForPartition(int partition)
    {
        checkArgument(partition < mapOutputMetadata.length, "Trying to read shuffle metadata for partition: partition > mapoutput.size()");
        return mapOutputMetadata[partition];
    }
}
