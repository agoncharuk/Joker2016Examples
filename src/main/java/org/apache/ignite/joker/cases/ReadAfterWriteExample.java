/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.joker.cases;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.joker.model.Account;

import java.math.BigDecimal;

/**
 * This example demonstrates failure to read a previously written value in certain configurations.
 */
public class ReadAfterWriteExample {
    /**
     * @param args Main arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try(Ignite ignite = Ignition.start("config/server.xml")) {
            IgniteCache<Long, Account> cache = ignite.getOrCreateCache(
                    cacheConfiguration(CacheWriteSynchronizationMode.PRIMARY_SYNC, true));

            try {
                int nulls = 0;

                final int cnt = 100_000;

                for (long i = 0; i < cnt; i++) {
                    cache.put(i, new Account(i, new BigDecimal(i)));

                    Account read = cache.get(i);

                    if (read == null) {
                        System.out.println("Read null for account ID: " + i);

                        nulls++;

                        if (i > 0 && i % 50_000 == 0)
                            System.out.println("Done: " + i);
                    }
                }

                System.out.printf("Percentage of failed reads: %.2f%%\n", (double)nulls * 100 / cnt);
            }
            finally {
                ignite.destroyCache(null);
            }
        }
    }

    /**
     * @param syncMode Write synchronization mode.
     * @param readFromBackup Read from backup flag.
     * @return Cache configuration.
     */
    private static CacheConfiguration<Long, Account> cacheConfiguration(
        CacheWriteSynchronizationMode syncMode,
        boolean readFromBackup
    ) {
        CacheConfiguration<Long, Account> cfg = new CacheConfiguration<>();

        cfg.setBackups(2);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setWriteSynchronizationMode(syncMode);
        cfg.setReadFromBackup(readFromBackup);

        return cfg;
    }
}
