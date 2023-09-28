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
package com.facebook.presto.execution.scheduler.mapreduce.exchange;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.exchange.ExchangeProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileBasedExchangeProvider
        implements ExchangeProvider
{
    private static final String FILE_EXCHANGE_STORAGE_BASE_DIR = "/tmp/local_exchange";
    private final Map<String, FileBasedExchangeJob> exchangeJobMap;

    public FileBasedExchangeProvider()
    {
        exchangeJobMap = new HashMap<>();
    }

    @Override
    public String registerExchange(String stageExecutionId, int mapperCount, int reducerCount, Map<String, String> properties)
            throws IOException
    {
        FileBasedExchangeJob fileBasedExchangeJob = new FileBasedExchangeJob(
                stageExecutionId,
                reducerCount,
                FILE_EXCHANGE_STORAGE_BASE_DIR);
        String exchangeId = stageExecutionId.replace(".", "_");
        exchangeJobMap.put(exchangeId, fileBasedExchangeJob);
        return exchangeId;
    }

    @Override
    public Object generateExchangeReadMetadata(
            String exchangeId,
            List<Integer> partitionIdsToRead,
            List<Object> partitionsMetadata)
    {
        FileBasedExchangeJob fileBasedExchangeJob = exchangeJobMap.get(exchangeId);
        HashMap<String, Object> exchangeProps = new HashMap<>();
        exchangeProps.put("basePath", fileBasedExchangeJob.getRootPath());
        exchangeProps.put("exchangeId", exchangeId);
        exchangeProps.put("partitionIds", partitionIdsToRead.stream().map(String::valueOf).collect(Collectors.toList()));

        JsonCodec<Map<String, Object>> codec = JsonCodec.mapJsonCodec(String.class, Object.class);
        return codec.toJson(exchangeProps);
    }

    @Override
    public Object generateExchangeWriteMetadata(String exchangeId, int shuffleTaskId)
    {
        FileBasedExchangeJob fileBasedExchangeJob = exchangeJobMap.get(exchangeId);
        HashMap<String, Object> shuffleProps = new HashMap<>();
        shuffleProps.put("basePath", fileBasedExchangeJob.getRootPath());
        shuffleProps.put("exchangeId", exchangeId);
        shuffleProps.put("writerId", String.valueOf(shuffleTaskId));
        shuffleProps.put("numPartitions", fileBasedExchangeJob.getNumPartitions());

        JsonCodec<Map<String, Object>> codec = JsonCodec.mapJsonCodec(String.class, Object.class);
        return codec.toJson(shuffleProps);
    }

    @Override
    public Object generateExchangeWriteOutputMetadata(String exchangeId, int shuffleTaskId)
    {
        return null;
    }

    @Override
    public void startWriters(String stageExecutionId)
            throws IOException {}

    @Override
    public void startReaders(String stageExecutionId) {}

    @Override
    public void closeExchange(String stageExecutionId)
            throws IOException {}

    public static class FileBasedExchangeJob
    {
        private final String stageExecutionId;

        public String getString()
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

        public FileBasedExchangeJob(String stageExecutionId, int numPartitions, String rootPath)
        {
            this.stageExecutionId = stageExecutionId;
            this.queryId = stageExecutionId.split("\\.")[0];
            this.numPartitions = numPartitions;
            this.rootPath = rootPath;
        }
    }
}
