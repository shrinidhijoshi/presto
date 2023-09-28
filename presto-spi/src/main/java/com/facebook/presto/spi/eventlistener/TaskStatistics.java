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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.common.RuntimeStats;

import java.time.Duration;

import static io.airlift.slice.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskStatistics
{
    
    private final Duration runTime;
    private final Duration elapsedTimeInNanos;
    private final Duration queuedTimeInNanos;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int queuedPartitionedDrivers;
    private final long queuedPartitionedSplitsWeight;
    private final int runningDrivers;
    private final int runningPartitionedDrivers;
    private final long runningPartitionedSplitsWeight;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final long peakUserMemoryInBytes;
    private final long peakTotalMemoryInBytes;
    private final long peakNodeTotalMemoryInBytes;

    private final long totalScheduledTimeInNanos;
    private final long totalCpuTimeInNanos;
    private final long totalBlockedTimeInNanos;
    private final boolean fullyBlocked;
    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;

    private final long processedInputDataSizeInBytes;
    private final long processedInputPositions;

    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final int fullGcCount;
    private final long fullGcTimeInMillis;

    // RuntimeStats aggregated at the task level including the metrics exposed in this task and each operator of this task.
    private final RuntimeStats runtimeStats;

    public TaskStatistics(
            Duration runTime,
            Duration elapsedTimeInNanos,
            Duration queuedTimeInNanos,

            int totalDrivers,
            int queuedDrivers,
            int queuedPartitionedDrivers,
            long queuedPartitionedSplitsWeight,
            int runningDrivers,
            int runningPartitionedDrivers,
            long runningPartitionedSplitsWeight,
            int blockedDrivers,
            int completedDrivers,

            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            long userMemoryReservationInBytes,
            long revocableMemoryReservationInBytes,
            long systemMemoryReservationInBytes,

            long peakTotalMemoryInBytes,
            long peakUserMemoryInBytes,
            long peakNodeTotalMemoryInBytes,

            long totalScheduledTimeInNanos,
            long totalCpuTimeInNanos,
            long totalBlockedTimeInNanos,
            boolean fullyBlocked,

            long totalAllocationInBytes,

            long rawInputDataSizeInBytes,
            long rawInputPositions,

            long processedInputDataSizeInBytes,
            long processedInputPositions,

            long outputDataSizeInBytes,
            long outputPositions,

            long physicalWrittenDataSizeInBytes,

            int fullGcCount,
            long fullGcTimeInMillis,

            RuntimeStats runtimeStats)
    {

        this.runTime = runTime;
        this.elapsedTimeInNanos = elapsedTimeInNanos;
        this.queuedTimeInNanos = queuedTimeInNanos;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(queuedPartitionedSplitsWeight >= 0, "queuedPartitionedSplitsWeight must be positive");
        this.queuedPartitionedSplitsWeight = queuedPartitionedSplitsWeight;

        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(runningPartitionedSplitsWeight >= 0, "runningPartitionedSplitsWeight must be positive");
        this.runningPartitionedSplitsWeight = runningPartitionedSplitsWeight;

        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;

        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        this.revocableMemoryReservationInBytes = revocableMemoryReservationInBytes;
        this.systemMemoryReservationInBytes = systemMemoryReservationInBytes;

        this.peakTotalMemoryInBytes = peakTotalMemoryInBytes;
        this.peakUserMemoryInBytes = peakUserMemoryInBytes;
        this.peakNodeTotalMemoryInBytes = peakNodeTotalMemoryInBytes;

        this.totalScheduledTimeInNanos = totalScheduledTimeInNanos;
        this.totalCpuTimeInNanos = totalCpuTimeInNanos;
        this.totalBlockedTimeInNanos = totalBlockedTimeInNanos;
        this.fullyBlocked = fullyBlocked;

        this.totalAllocationInBytes = totalAllocationInBytes;

        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSizeInBytes = processedInputDataSizeInBytes;
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSizeInBytes = outputDataSizeInBytes;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSizeInBytes = physicalWrittenDataSizeInBytes;

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTimeInMillis = fullGcTimeInMillis;

        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

   
    public DateTime getCreateTime()
    {
        return createTime;
    }


    public DateTime getFirstStartTime()
    {
        return firstStartTime;
    }


    public DateTime getLastStartTime()
    {
        return lastStartTime;
    }


    public DateTime getLastEndTime()
    {
        return lastEndTime;
    }


    public DateTime getEndTime()
    {
        return endTime;
    }

   
    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

   
    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }

   
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

   
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

   
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

   
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

   
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

   
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

   
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

   
    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

   
    public long getRevocableMemoryReservationInBytes()
    {
        return revocableMemoryReservationInBytes;
    }

   
    public long getSystemMemoryReservationInBytes()
    {
        return systemMemoryReservationInBytes;
    }

   
    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemoryInBytes;
    }

   
    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemoryInBytes;
    }

   
    public long getPeakNodeTotalMemoryInBytes()
    {
        return peakNodeTotalMemoryInBytes;
    }

   
    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

   
    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

   
    public long getTotalBlockedTimeInNanos()
    {
        return totalBlockedTimeInNanos;
    }

   
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

   
    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

   
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

   
    public long getProcessedInputDataSizeInBytes()
    {
        return processedInputDataSizeInBytes;
    }

   
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

   
    public long getOutputDataSizeInBytes()
    {
        return outputDataSizeInBytes;
    }

   
    public long getOutputPositions()
    {
        return outputPositions;
    }

   
    public long getPhysicalWrittenDataSizeInBytes()
    {
        return physicalWrittenDataSizeInBytes;
    }

    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

   
    public long getQueuedPartitionedSplitsWeight()
    {
        return queuedPartitionedSplitsWeight;
    }

   
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

   
    public long getRunningPartitionedSplitsWeight()
    {
        return runningPartitionedSplitsWeight;
    }

   
    public int getFullGcCount()
    {
        return fullGcCount;
    }

   
    public long getFullGcTimeInMillis()
    {
        return fullGcTimeInMillis;
    }

   
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
