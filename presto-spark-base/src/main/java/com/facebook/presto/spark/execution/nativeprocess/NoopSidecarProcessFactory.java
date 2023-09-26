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
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class NoopSidecarProcessFactory
        implements SidecarProcessFactory
{
    private static final Logger log = Logger.get(NoopSidecarProcessFactory.class);
    private static SidecarProcess process;

    @Inject
    public NoopSidecarProcessFactory() {}

    @Override
    public synchronized void launchSidecarProcessIfApplicable(
            Session session,
            PlanFragment planFragment,
            TaskId taskId)
    {
        if (process != null) {
            return;
        }

        Optional<List<String>> command = getSidecarProcessLaunchCommandIfApplicable(
                session, planFragment, taskId);

        if (!command.isPresent()) {
            return;
        }

        process = new SidecarProcess(command.get());
        try {
            process.start();
        }
        catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void killSidecarProcessIfApplicable()
    {
        if (process == null) {
            return;
        }

        log.info("killing sidecar process");
        process.close();
        process = null;
    }

    private Optional<List<String>> getSidecarProcessLaunchCommandIfApplicable(
            Session session,
            PlanFragment planFragment,
            TaskId taskId)
    {
        return Optional.of(ImmutableList.of(
                "/tmp/sample_profiler",
                "[Sidecar Process] user=" + session.getUser() + " fragmentId=" + planFragment.getId() + " taskId=" + taskId));
    }

    @Override
    public void close()
            throws IOException
    {
        if (process != null) {
            process.close();
        }
    }
//
//    private String getProcessWorkingPath(String path)
//    {
//        File absolutePath = new File(path);
//        if (!absolutePath.isAbsolute()) {
//            // In the case of SparkEnv is not initialed (e.g. unit test), we just use current location instead of calling SparkFiles.getRootDirectory() to avoid error.
//            String rootDirectory = SparkEnv$.MODULE$.get() != null ? SparkFiles.getRootDirectory() : ".";
//            absolutePath = new File(rootDirectory, path);
//        }
//
//        if (!absolutePath.exists()) {
//            log.error(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
//            throw new PrestoException(NATIVE_EXECUTION_BINARY_NOT_EXIST, format("File doesn't exist %s", absolutePath.getAbsolutePath()));
//        }
//
//        return absolutePath.getAbsolutePath();
//    }
//
//    private List<String> getLaunchCommand(TaskId taskId, Session session)
//    {
//        String executablePath = getProcessWorkingPath(SystemSessionProperties.getSidecarExecutablePath(session));
//        String programArgs = SystemSessionProperties.getSidecarProgramArguments(session);
//        ImmutableList.Builder<String> command = ImmutableList.builder();
//        List<String> argsList = Arrays.asList(programArgs.split("\\s+"));
//        argsList.add(format("--sample-tags %s", taskId));
//        command.add(executablePath).addAll(argsList);
//        ImmutableList<String> commandList = command.build();
//        log.info("Launching sidecar process using command: %s %s", executablePath, String.join(" ", commandList));
//        return commandList;
//    }
}
