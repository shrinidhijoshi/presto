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
package com.facebook.presto.execution.scheduler.mapreduce.exchange;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.exchange.ExchangeProvider;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Set;

public class ExchangeDependency
{
    private static final Logger log = Logger.get(ExchangeDependency.class);
    private String exchangeId;
    private int numMappers;
    private int numReducers;
    private Set<String> writerIds;

    private final ExchangeProvider exchangeProvider;
    private final PlanFragment writerFragment;
    private PlanFragment readerFragment;
    public ExchangeDependency(String exchangeId, int numMappers, int numReducers, PlanFragment writerFragment, ExchangeProvider exchangeProvider)
    {
        this.writerFragment = writerFragment;
        this.exchangeProvider = exchangeProvider;

        this.exchangeId = exchangeId;
        try {
            exchangeProvider.registerExchange(
                    exchangeId,
                    numMappers,
                    numReducers,
                    ImmutableMap.of("schema", "test"));
        }
        catch (IOException ex) {
            log.error("Error creating shuffleJob: ", ex);
            throw new RuntimeException(ex);
        }
    }

    public void setReaderFragment(PlanFragment planFragment)
    {
        this.readerFragment = readerFragment;
    }

    public void setNumMappers(int numMappers)
    {
        this.numMappers = numMappers;
    }

    public void setNumReducers(int numReducers)
    {
        this.numReducers = numReducers;
    }

    public void addWriterId(String writerId)
    {
        this.writerIds.add(writerId);
    }

    public void remoteWriterId(String writerId)
    {
        this.writerIds.remove(writerId);
    }

    public void setExchangeId(String exchangeId)
    {
        this.exchangeId = exchangeId;
    }

    public String getExchangeId()
    {
        return exchangeId;
    }

    public PlanFragment getWriterFragment()
    {
        return writerFragment;
    }
}
