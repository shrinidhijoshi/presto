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
import com.facebook.presto.sql.analyzer.FeaturesConfig;

import javax.inject.Inject;

public class FileBasedExchangeProviderFactory
        implements ExchangeProviderFactory
{
    private static final String NAME = "file";
    private final FeaturesConfig featuresConfig;
    @Inject
    public FileBasedExchangeProviderFactory(
            ExchangeProviderRegistry exchangeProviderRegistry,
            FeaturesConfig featuresConfig)
    {
        this.featuresConfig = featuresConfig;
        exchangeProviderRegistry.addExchangeProvider(this);
    }

    public ExchangeProvider get()
    {
        return new FileBasedExchangeProvider(featuresConfig.getShuffleBasePath());
    }

    @Override
    public String getName()
    {
        return NAME;
    }
}
