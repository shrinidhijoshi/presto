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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFailure;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import org.apache.spark.SparkConf;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.PrestoSparkNativeQueryRunnerUtils.FALLBACK_SPARK_SHUFFLE_MANAGER;
import static com.facebook.presto.spark.PrestoSparkNativeQueryRunnerUtils.SPARK_SHUFFLE_MANAGER;
import static com.facebook.presto.spark.PrestoSparkNativeQueryRunnerUtils.getNativeExecutionShuffleConfigs;

public class PrestoSparkNativeQueryRunner
        extends PrestoSparkQueryRunner
{
    private static final Logger log = Logger.get(PrestoSparkNativeQueryRunner.class);

    public PrestoSparkNativeQueryRunner(String defaultCatalog, Map<String, String> additionalConfigProperties, Map<String, String> hiveProperties, Map<String, String> additionalSparkProperties, Optional<Path> dataDirectory, ImmutableList<Module> additionalModules, int availableCpuCount) {
        super(defaultCatalog, additionalConfigProperties, hiveProperties, additionalSparkProperties, dataDirectory, additionalModules, availableCpuCount);
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        // SparkConf before
        SparkConf sc = getSparkContext().conf();
//        String oldShuffleManager = sc.get(SPARK_SHUFFLE_MANAGER);
//        String oldFallbackShuffleManager = sc.get(FALLBACK_SPARK_SHUFFLE_MANAGER);
        getNativeExecutionShuffleConfigs().forEach(sc::set);
        try {
            return executeWithStrategies(session, sql, getExecutionStrategies(session));
        }
        catch (PrestoSparkFailure failure) {
            if (!failure.getRetryExecutionStrategies().isEmpty()) {
                return executeWithStrategies(session, sql, failure.getRetryExecutionStrategies());
            }

            throw failure;
        } finally {
            sc.remove(SPARK_SHUFFLE_MANAGER);
            sc.remove(FALLBACK_SPARK_SHUFFLE_MANAGER);
        }
    }
}
