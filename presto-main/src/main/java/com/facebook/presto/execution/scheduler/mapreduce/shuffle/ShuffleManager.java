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
package com.facebook.presto.execution.scheduler.mapreduce.shuffle;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.TaskInfo;

import java.io.IOException;
import java.util.List;

public interface ShuffleManager
{
    void registerShuffle(Session session, StageExecutionId stageExecutionId, int mapperCount, int reducerCount)
            throws IOException;

    Object generateShuffleReadMetadata(StageExecutionId stageExecutionId, List<Integer> partitionIdsToRead, List<Object> partitionsMetadata);

    Object generateShuffleWriteMetadata(StageExecutionId stageExecutionId, int shuffleTaskId);
    Object generateShuffleWriteOutputMetadata(TaskInfo taskInfo, int shuffleTaskId);

    void startWriters(StageExecutionId stageExecutionId)
            throws IOException;

    void startReaders(StageExecutionId stageExecutionId);

    void closeShuffle(StageExecutionId stageExecutionId)
            throws IOException;
}
