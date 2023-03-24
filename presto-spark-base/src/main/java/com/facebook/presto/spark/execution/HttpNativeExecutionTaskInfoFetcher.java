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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * This class helps to fetch {@link TaskInfo} for a native task through HTTP communications with a Presto worker. Upon calling start(), object of this class will continuously poll
 * {@link TaskInfo} from Presto worker and update its internal {@link TaskInfo} buffer. Caller is responsible for retrieving updated {@link TaskInfo} by calling getTaskInfo()
 * method.
 * Caller is also responsible for calling stop() to release resource when this fetcher is no longer needed.
 */
public class HttpNativeExecutionTaskInfoFetcher
{
    private static final Logger log = Logger.get(HttpNativeExecutionTaskInfoFetcher.class);

    private final PrestoSparkHttpTaskClient workerClient;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final Executor executor;
    private final Duration infoFetchInterval;

    private CompletableFuture<TaskInfo> taskInfoFuture;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    public HttpNativeExecutionTaskInfoFetcher(
            ScheduledExecutorService updateScheduledExecutor,
            PrestoSparkHttpTaskClient workerClient,
            Executor executor,
            Duration infoFetchInterval)
    {
        this.workerClient = requireNonNull(workerClient, "workerClient is null");
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.infoFetchInterval = requireNonNull(infoFetchInterval, "infoFetchInterval is null");
    }

    public CompletableFuture<TaskInfo> start()
    {
        final CompletableFuture<TaskInfo> taskInfoFuture = new CompletableFuture<>();
        try {
            ListenableFuture<BaseResponse<TaskInfo>> taskInfoFutureInternal = workerClient.getTaskInfo();
            Futures.addCallback(
                    taskInfoFutureInternal,
                    new FutureCallback<BaseResponse<TaskInfo>>()
                    {
                        @Override
                        public void onSuccess(BaseResponse<TaskInfo> result)
                        {
                            log.debug("TaskInfoCallback success %s", result.getValue().getTaskId());
                            taskInfoFuture.complete(result.getValue());
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            log.error("TaskInfoCallback failed %s", t);
                            taskInfoFuture.completeExceptionally(t);
                            workerClient.abortResults();
                        }
                    },
                    executor);
        }
        catch (Throwable t) {
            throw t;
        }

        return taskInfoFuture;
    }

    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    public Optional<TaskInfo> getTaskInfo()
    {
        try {
            // Wait for taskInfo to be obtained
            return Optional.of(taskInfoFuture.get());
        }
        catch (ExecutionException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public CompletableFuture<TaskInfo> startAndWaitForCompletion()
    {
        CompletableFuture<TaskInfo> taskInfoCompletableFuture = new CompletableFuture<>();
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() ->
        {
            try {
                ListenableFuture<BaseResponse<TaskInfo>> taskInfoFutureInternal = workerClient.getTaskInfo();
                Futures.addCallback(
                        taskInfoFutureInternal,
                        new FutureCallback<BaseResponse<TaskInfo>>()
                        {
                            @Override
                            public void onSuccess(BaseResponse<TaskInfo> result)
                            {
                                BufferState state = result.getValue().getOutputBuffers().getState();
                                log.debug("startAndWaitForCompletion: success taskId=%s, taskState=%s, bufferState=%s", result.getValue().getTaskId());
                                if (ImmutableList.of(BufferState.OPEN, BufferState.FLUSHING, TaskState.CANCELED, BufferState.FINISHED, BufferState.FAILED)
                                        .contains(state)) {
                                    taskInfoCompletableFuture.complete(result.getValue());
                                }

                                log.info("startAndWaitForCompletion: TaskState is still in non-final state.. state=%s", state);
                            }

                            @Override
                            public void onFailure(Throwable t)
                            {
                                log.error("startAndWaitForCompletion: TaskInfoCallback failed %s", t);
                                taskInfoCompletableFuture.completeExceptionally(t);
                            }
                        },
                        executor);
            }
            catch (Throwable t) {
                throw t;
            }
        }, 0, (long) infoFetchInterval.getValue(), infoFetchInterval.getUnit());

        return taskInfoCompletableFuture;
    }
}
