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
package com.facebook.presto.spi.worker;

import com.facebook.presto.spi.QueryId;

import java.util.Map;

public interface WorkerProvider
{
    void start();
    String registerQuery(QueryId queryId, String namespace);
    void updateWorkerRequirements(QueryId queryId, WorkerSpec workerSpec, int count);
    Map<String, Map<String, Map<String, Long>>> getWorkerStats(QueryId queryId);
    String unregisterQuery(QueryId queryId);
    void close();
}
