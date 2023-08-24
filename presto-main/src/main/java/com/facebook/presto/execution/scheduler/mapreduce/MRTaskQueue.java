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

import com.facebook.presto.execution.RemoteTask;

import javax.inject.Inject;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class MRTaskQueue
{
    private LinkedBlockingDeque<RemoteTask> taskQueue;

    @Inject
    MRTaskQueue()
    {
        this.taskQueue = new LinkedBlockingDeque<>();
    }
    public RemoteTask addTask(RemoteTask remoteTask)
    {
        this.taskQueue.add(remoteTask);
        return remoteTask;
    }

    public int size()
    {
        return taskQueue.size();
    }

    public RemoteTask poll()
            throws InterruptedException
    {
        return taskQueue.poll(10, TimeUnit.MILLISECONDS);
    }

    public RemoteTask peek()
    {
        return taskQueue.peek();
    }
}
