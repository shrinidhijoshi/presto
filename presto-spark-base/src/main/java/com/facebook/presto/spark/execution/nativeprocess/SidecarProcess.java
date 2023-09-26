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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.lang.String.format;

public class SidecarProcess
        implements AutoCloseable
{
    private static final Logger log = Logger.get(SidecarProcess.class);

    private final List<String> launchArgs;
    private Process process;

    public SidecarProcess(List<String> launchArgs)
    {
        this.launchArgs = launchArgs;
    }

    /**
     * Starts the external native execution process. The method will be blocked by connecting to the native process's /v1/info endpoint with backoff retries until timeout.
     */
    public synchronized void start()
            throws ExecutionException, InterruptedException, IOException
    {
        if (process != null && process.isAlive()) {
            return;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(launchArgs);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            process = processBuilder.start();
        }
        catch (IOException e) {
            log.error(format("Cannot start %s, error message: %s", processBuilder.command(), e.getMessage()));
            throw new PrestoException(NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR, format("Cannot start %s", processBuilder.command()), e);
        }
    }

    @Override
    public void close()
    {
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                process.waitFor(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                if (process.isAlive()) {
                    log.warn("Graceful shutdown of sidecar execution process failed. Force killing it.");
                    process.destroyForcibly();
                }
            }
        }
        process = null;
    }

    public boolean isAlive()
    {
        return process != null && process.isAlive();
    }
}
