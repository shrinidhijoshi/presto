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
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.spi.page.SerializedPage;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class MRTableCommitMetadataCache
{
    private static Logger logger = Logger.get(MRTableCommitMetadataCache.class);

    private LinkedBlockingDeque<SerializedPage> taskIdToResultPagesCache;
    @Inject
    public MRTableCommitMetadataCache()
    {
        taskIdToResultPagesCache = new LinkedBlockingDeque<>();
    }

    public List<SerializedPage> getTableCommitMetadata()
    {
        synchronized (this) {
            List<SerializedPage> result = new ArrayList<>();
            taskIdToResultPagesCache.drainTo(result);
            return result;
        }
    }

    public void putResultsForTask(StageExecutionId stageExecutionId, List<SerializedPage> serializedPage)
    {
        synchronized (this) {
            taskIdToResultPagesCache.addAll(serializedPage);
            logger.info("Added %s pages to result cache", serializedPage.size());
        }
    }
}
