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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.TaskInfo;

import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getShuffleBasePath;

public class LocalShuffleManager
        implements ShuffleManager
{
    private final Map<StageExecutionId, LocalShuffleJob> shuffleJobMap;

    @Inject
    public LocalShuffleManager()
    {
        shuffleJobMap = new HashMap<>();
    }

    @Override
    public void registerShuffle(Session session, StageExecutionId stageExecutionId, int mapperCount, int reducerCount)
            throws IOException
    {
        LocalShuffleJob localShuffleJob = new LocalShuffleJob(
                stageExecutionId,
                stageExecutionId.getStageId().getQueryId().toString(),
                reducerCount,
                getShuffleBasePath(session));
        shuffleJobMap.put(stageExecutionId, localShuffleJob);
    }

    @Override
    public Object generateShuffleReadMetadata(
            StageExecutionId stageExecutionId,
            List<Integer> partitionIdsToRead,
            List<Object> partitionsMetadata)
    {
        LocalShuffleJob localShuffleJob = shuffleJobMap.get(stageExecutionId);
        HashMap<String, Object> shuffleProps = new HashMap<>();
        shuffleProps.put("rootPath", localShuffleJob.getRootPath());
        shuffleProps.put("partitionIds", partitionIdsToRead
                .stream().map(i -> "shuffle_" + stageExecutionId.getStageId().getId() + "_0_" + i + "_0")
                .collect(Collectors.toList()));
        shuffleProps.put("queryId", localShuffleJob.getQueryId());
        shuffleProps.put("numPartitions", localShuffleJob.getNumPartitions());

        JsonCodec<Map<String, Object>> codec = JsonCodec.mapJsonCodec(String.class, Object.class);
        return codec.toJson(shuffleProps);
    }

    @Override
    public Object generateShuffleWriteMetadata(StageExecutionId stageExecutionId, int shuffleTaskId)
    {
        LocalShuffleJob localShuffleJob = shuffleJobMap.get(stageExecutionId);
        HashMap<String, Object> shuffleProps = new HashMap<>();
        shuffleProps.put("rootPath", localShuffleJob.getRootPath());
        shuffleProps.put("shuffleId", localShuffleJob.getStageExecutionId().getStageId().getId());
        shuffleProps.put("queryId", localShuffleJob.getQueryId());
        shuffleProps.put("numPartitions", localShuffleJob.getNumPartitions());

        JsonCodec<Map<String, Object>> codec = JsonCodec.mapJsonCodec(String.class, Object.class);
        return codec.toJson(shuffleProps);
    }

    @Override
    public Object generateShuffleWriteOutputMetadata(TaskInfo taskInfo, int shuffleTaskId)
    {
        return null;
    }

    @Override
    public void startWriters(StageExecutionId stageExecutionId)
            throws IOException {}

    @Override
    public void startReaders(StageExecutionId stageExecutionId) {}

    @Override
    public void closeShuffle(StageExecutionId stageExecutionId)
            throws IOException {}

    public static class LocalShuffleJob
    {
        private final StageExecutionId stageExecutionId;

        public StageExecutionId getStageExecutionId()
        {
            return stageExecutionId;
        }

        public String getQueryId()
        {
            return queryId;
        }

        public int getNumPartitions()
        {
            return numPartitions;
        }

        public String getRootPath()
        {
            return rootPath;
        }

        private final String queryId;
        private final int numPartitions;
        private final String rootPath;

        public LocalShuffleJob(StageExecutionId stageExecutionId, String queryId, int numPartitions, String rootPath)
        {
            this.stageExecutionId = stageExecutionId;
            this.queryId = queryId;
            this.numPartitions = numPartitions;
            this.rootPath = rootPath;
        }
    }
}
