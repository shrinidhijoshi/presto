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

import com.facebook.presto.spi.exchange.ExchangeProvider;
import com.facebook.presto.spi.exchange.ExchangeProviderFactory;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

public class ExchangeProviderRegistry
{
    private final Map<String, ExchangeProvider> exchangeProviderMap;

    @Inject
    public ExchangeProviderRegistry()
    {
        this.exchangeProviderMap = new HashMap<>();
    }
    public void addExchangeProvider(ExchangeProviderFactory exchangeProviderFactory)
    {
        this.exchangeProviderMap.put(exchangeProviderFactory.getName(), exchangeProviderFactory.get());
    }

    public ExchangeProvider get(String name)
    {
        return exchangeProviderMap.get(name);
    }
}
