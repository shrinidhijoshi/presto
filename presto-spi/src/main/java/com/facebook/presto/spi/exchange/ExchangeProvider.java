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
package com.facebook.presto.spi.exchange;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ExchangeProvider
{
    String registerExchange(String stageExecutionId, int mapperCount, int reducerCount, Map<String, String> properties)
            throws IOException;

    Object generateExchangeReadMetadata(String stageExecutionId, List<Integer> partitionIdsToRead, List<Object> partitionsMetadata);

    Object generateExchangeWriteMetadata(String exchangeId, int shuffleTaskId);
    Object generateExchangeWriteOutputMetadata(String exchangeId, int shuffleTaskId);

    void startWriters(String exchangeId)
            throws IOException;

    void startReaders(String exchangeId);

    void closeExchange(String stageExecutionId)
            throws IOException;
}
