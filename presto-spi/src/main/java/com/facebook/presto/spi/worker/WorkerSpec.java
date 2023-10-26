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

public class WorkerSpec
{
    long cpus;
    long memoryBytes;

    public WorkerSpec(long cpus, long memoryBytes)
    {
        this.cpus = cpus;
        this.memoryBytes = memoryBytes;
    }
    public long getCpus()
    {
        return cpus;
    }

    public void setCpus(long cpus)
    {
        this.cpus = cpus;
    }

    public long getMemoryBytes()
    {
        return memoryBytes;
    }

    public void setMemoryBytes(long memoryBytes)
    {
        this.memoryBytes = memoryBytes;
    }
}
