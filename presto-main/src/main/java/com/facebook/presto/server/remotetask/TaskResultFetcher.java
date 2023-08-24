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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.HttpRpcShuffleClient;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SERIALIZED_PAGE_CHECKSUM_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.isChecksumValid;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TaskResultFetcher
{
    private static final Logger log = Logger.get(TaskResultFetcher.class);
    private static final String TASK_URI = "/v1/task/";

    private static final Duration FETCH_INTERVAL = new Duration(200, TimeUnit.MILLISECONDS);
    private static final Duration POLL_TIMEOUT = new Duration(100, TimeUnit.MILLISECONDS);
    private static final DataSize MAX_RESPONSE_SIZE = new DataSize(32, DataSize.Unit.MEGABYTE);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(128, DataSize.Unit.MEGABYTE);
    private static final String TASK_ERROR_MESSAGE = "TaskResultsFetcher encountered too many errors talking to native process.";
    private final Executor executor;
    private final URI location;
    private final URI taskUri;
    private final ScheduledExecutorService scheduler;
    private final LinkedBlockingDeque<SerializedPage> pageBuffer = new LinkedBlockingDeque<>();
    private final AtomicLong bufferMemoryBytes;
    private final Duration maxErrorDuration;
    private final RequestErrorTracker errorTracker;
    private final AtomicReference<RuntimeException> lastException = new AtomicReference<>();
    private final HttpClient httpClient;
    private ScheduledFuture<?> scheduledFuture;
    private long token;
    public TaskResultFetcher(
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            URI location,
            TaskId taskId,
            ScheduledExecutorService errorRetryScheduledExecutor,
            Executor executor,
            Duration maxErrorDuration)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.location = requireNonNull(location, "workerClient is null");
        this.bufferMemoryBytes = new AtomicLong();
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                TASK_ERROR_MESSAGE,
                maxErrorDuration,
                requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null"),
                "getting results from native process");
        this.taskUri = createTaskUri(location, taskId);
        this.httpClient = httpClient;
    }

    public void start()
    {
        scheduledFuture = scheduler.scheduleAtFixedRate(this::doGetResults,
                0,
                (long) FETCH_INTERVAL.getValue(),
                FETCH_INTERVAL.getUnit());
    }

    public void stop(boolean success)
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        if (success && !pageBuffer.isEmpty()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("TaskResultFetcher is closed with %s pages left in the buffer", pageBuffer.size()));
        }
    }

    /**
     * Blocking call to poll from result buffer. Blocks until content becomes
     * available in the buffer, or until timeout is hit.
     *
     * @return the first {@link SerializedPage} result buffer contains.
     */
    public Optional<SerializedPage> pollPage()
            throws InterruptedException
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }

        SerializedPage page = pageBuffer.poll((long) POLL_TIMEOUT.getValue(), POLL_TIMEOUT.getUnit());
        if (page != null) {
            bufferMemoryBytes.addAndGet(-page.getSizeInBytes());
            return Optional.of(page);
        }
        return Optional.empty();
    }

    public void extractResultInto(List<SerializedPage> resultContainer)
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }

        pageBuffer.drainTo(resultContainer);
        resultContainer.forEach(serializedPage -> {
            bufferMemoryBytes.addAndGet(-serializedPage.getSizeInBytes());
        });
    }

    public boolean hasPage()
    {
        if (scheduledFuture != null && scheduledFuture.isCancelled() && lastException.get() != null) {
            throw lastException.get();
        }

        return !pageBuffer.isEmpty();
    }

    private void doGetResults()
    {
        if (bufferMemoryBytes.longValue() >= MAX_BUFFER_SIZE.toBytes()) {
            return;
        }

        try {
            PageBufferClient.PagesResponse pagesResponse = getResults(token, MAX_RESPONSE_SIZE).get();
            onSuccess(pagesResponse);
        }
        catch (Throwable t) {
            onFailure(t);
        }
    }

    private void onSuccess(PageBufferClient.PagesResponse pagesResponse)
    {
        errorTracker.requestSucceeded();

        List<SerializedPage> pages = pagesResponse.getPages();
        long bytes = 0;
        long positionCount = 0;
        for (SerializedPage page : pages) {
            if (!isChecksumValid(page)) {
                throw new PrestoException(
                        SERIALIZED_PAGE_CHECKSUM_ERROR,
                        format("Received corrupted serialized page from host %s",
                                HostAddress.fromUri(location)));
            }
            bytes += page.getSizeInBytes();
            positionCount += page.getPositionCount();
        }
        log.info("Received %s rows in %s pages from %s", positionCount, pages.size(), location);

        pageBuffer.addAll(pages);
        bufferMemoryBytes.addAndGet(bytes);
        long nextToken = pagesResponse.getNextToken();
        if (pages.size() > 0) {
            acknowledgeResultsAsync(nextToken);
        }
        token = nextToken;
        if (pagesResponse.isClientComplete()) {
            abortResults();
            scheduledFuture.cancel(false);
        }
    }

    private void onFailure(Throwable t)
    {
        // record failure
        try {
            errorTracker.requestFailed(t);
        }
        catch (PrestoException e) {
            // Entering here means that we are unable to get any results from the CPP process
            // likely because process has crashed.
            abortResults();
            stop(false);
            lastException.set(e);
            return;
        }
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        try {
            // synchronously wait on throttling
            errorRateLimit.get(maxErrorDuration.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            // throttling error is not fatal, just log the error.
            log.debug(e.getMessage());
        }
    }

    /**
     * Get results from a native engine task that ends with none shuffle operator. It always fetches from a single buffer.
     */
    private ListenableFuture<PageBufferClient.PagesResponse> getResults(
            long token,
            DataSize maxResponseSize)
    {
        URI uri = uriBuilderFrom(taskUri)
                .appendPath("/results/0")
                .appendPath(String.valueOf(token))
                .build();
        return httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri)
                        .build(),
                new HttpRpcShuffleClient.PageResponseHandler());
    }

    private void acknowledgeResultsAsync(long nextToken)
    {
        URI uri = uriBuilderFrom(taskUri)
                .appendPath("/results/0")
                .appendPath(String.valueOf(nextToken))
                .appendPath("acknowledge")
                .build();
        httpExecuteAsync(prepareGet().setUri(uri).build(), null);
    }

    private ListenableFuture<?> abortResults()
    {
        return httpClient.executeAsync(
                prepareDelete().setUri(
                                uriBuilderFrom(taskUri)
                                        .appendPath("/results/0")
                                        .build())
                        .build(),
                createStatusResponseHandler());
    }

    private URI createTaskUri(URI baseUri, TaskId taskId)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(TASK_URI)
                .appendPath(taskId.toString())
                .build();
    }

    private <T> ListenableFuture<BaseResponse<T>> httpExecuteAsync(Request request, JsonCodec<T> codec)
    {
        return httpClient.executeAsync(request, new ResponseHandler<BaseResponse<T>, RuntimeException>()
        {
            @Override
            public BaseResponse<T> handleException(Request request, Exception exception)
            {
                throw propagate(request, exception);
            }

            @Override
            public BaseResponse<T> handle(Request request, Response response)
            {
                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                    throw new RuntimeException(format("Unexpected http response code: %s", response.getStatusCode()));
                }

                if (codec == null) {
                    return null;
                }

                try {
                    return createAdaptingJsonResponseHandler(codec).handle(request, response);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
