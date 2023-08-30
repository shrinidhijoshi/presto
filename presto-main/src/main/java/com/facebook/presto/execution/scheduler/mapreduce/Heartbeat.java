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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Heartbeat
{
    private final String nodeId;
    private final String nodeLocation;
    private final String uri;

    @JsonCreator
    public Heartbeat(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeLocation") String nodeLocation,
            @JsonProperty("uri") String uri)
    {
        this.nodeId = nodeId;
        this.nodeLocation = nodeLocation;
        this.uri = uri;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public String getNodeLocation()
    {
        return nodeLocation;
    }

    @JsonProperty
    public String getUri()
    {
        return uri;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Heartbeat that = (Heartbeat) o;
        return Objects.equals(nodeId, that.nodeId);
    }
}
