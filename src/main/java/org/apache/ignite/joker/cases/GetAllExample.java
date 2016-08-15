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
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.joker.model.Account;
import org.apache.ignite.transactions.Transaction;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * This example demonstrates failure to consistently read multiple values in certain scenarios.
 */
public class GetAllExample {
    /** */
    private static final int ACCOUNTS = 50;

    /** */
    private static final EntryProcessor<Long, Account, Account> GET_ENTRY_PROCESSOR =
        new EntryProcessor<Long, Account, Account>() {
            @Override public Account process(MutableEntry<Long, Account> entry, Object... arguments) {
                return entry.getValue();
            }
        };

    /**
     * @param args Main arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try(Ignite ignite = Ignition.start("config/server.xml")) {
            ignite.getOrCreateCache(cacheConfiguration(CacheWriteSynchronizationMode.PRIMARY_SYNC, true));

            int threadCnt = 4;

            ExecutorService svc = Executors.newFixedThreadPool(threadCnt);

            final AtomicBoolean stop = new AtomicBoolean(false);

            try {

                for (int i = 0; i < threadCnt; i++)
                    svc.submit(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            doLoad(stop, ignite);

                            return null;
                        }
                    });

                int failed = 0;
                int cnt = 10_000;

                for (int i = 0; i < cnt; i++) {
                    long accountId1 = ThreadLocalRandom.current().nextInt(ACCOUNTS) * 2;
                    long accountId2 = accountId1 + 1;

                    Set<Long> keys = new LinkedHashSet<>();

                    keys.add(accountId1);
                    keys.add(accountId2);

                    Map<Long, Account> all = getAllSafe2(keys, ignite);

                    Account account1 = all.get(accountId1);
                    Account account2 = all.get(accountId2);

                    if (account1 == null ^ account2 == null)
                        failed++;
                    else if (account1 != null) {
                        if (!account1.balance().add(account2.balance()).equals(BigDecimal.ZERO))
                            failed++;
                    }
                }

                System.out.printf("Percentage of failed reads: %.2f%%\n", (double)failed * 100 / cnt);
            }
            finally {
                stop.set(true);

                svc.shutdown();

                ignite.destroyCache(null);
            }
        }
    }

    /**
     * Reads keys from cache without locking.
     *
     * @param keys Keys to read.
     * @param ignite Ignite instance.
     * @return Read values.
     * @throws Exception If failed.
     */
    private static Map<Long, Account> getAll(Set<Long> keys, Ignite ignite) throws Exception {
        return ignite.<Long, Account>cache(null).getAll(keys);
    }

    /**
     * Reads keys from cache in pessimistic transaction.
     *
     * @param keys Keys to read.
     * @param ignite Ignite instance.
     * @return Read values.
     * @throws Exception If failed.
     */
    private static Map<Long, Account> getAllSafe1(Set<Long> keys, Ignite ignite) throws Exception {
        try (Transaction ignore = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            return ignite.<Long, Account>cache(null).getAll(keys);
        }
    }

    /**
     * Reads keys from cache using entry processor.
     *
     * @param keys Keys to read.
     * @param ignite Ignite instance.
     * @return Read values.
     * @throws Exception If failed.
     */
    private static Map<Long, Account> getAllSafe2(Set<Long> keys, Ignite ignite) throws Exception {
        Map<Long, EntryProcessorResult<Account>> resMap = ignite.<Long, Account>cache(null)
                .invokeAll(keys, GET_ENTRY_PROCESSOR);

        Map<Long, Account> res = new LinkedHashMap<>();

        for (Long key : keys) {
            EntryProcessorResult<Account> epRes = resMap.get(key);

            res.put(key, epRes == null ? null : epRes.get());
        }

        return res;
    }

    /**
     * @param stop Stop marker.
     * @param ignite Ignite instance.
     */
    private static void doLoad(final AtomicBoolean stop, Ignite ignite) {
        IgniteTransactions transactions = ignite.transactions();

        IgniteCache<Long, Account> cache = ignite.cache(null);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (!stop.get()) {
            try (Transaction tx = transactions.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                long accountId1 = rnd.nextInt(ACCOUNTS) * 2;
                long accountId2 = accountId1 + 1;

                Account account1 = cache.get(accountId1);
                Account account2 = cache.get(accountId2);

                int delta = rnd.nextInt(100);

                if (account1 == null) {
                    account1 = new Account(accountId1, new BigDecimal(delta));
                    account2 = new Account(accountId2, new BigDecimal(-delta));
                }
                else {
                    account1 = new Account(accountId1, new BigDecimal(delta).add(account1.balance()));
                    account2 = new Account(accountId2, new BigDecimal(-delta).add(account2.balance()));
                }

                cache.put(accountId1, account1);
                cache.put(accountId2, account2);

                tx.commit();
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
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(syncMode);
        cfg.setReadFromBackup(readFromBackup);

        return cfg;
    }
}
