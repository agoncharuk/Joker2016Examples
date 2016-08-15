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
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;

import java.math.BigDecimal;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * This examples demonstrates different scenarios for read-update-write scenarios.
 */
public class ReadUpdateWriteExample {
    /** */
    private static final int ACCOUNTS = 50;

    /**
     * @param args Main arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/server.xml")) {
            ignite.getOrCreateCache(cacheConfiguration(CacheWriteSynchronizationMode.PRIMARY_SYNC, true));

            int threadCnt = 4;

            ExecutorService svc = Executors.newFixedThreadPool(threadCnt);

            try {
                for (int i = 0; i < threadCnt; i++)
                    svc.submit(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            // PESSIMISTIC READ_COMMITTED - fails.
                            // PESSIMISTIC REPEATABLE_READ - ok.
                            // OPTIMISTIC REPEATABLE_READ - fails.
                            // OPTIMISTIC SERIALIZABLE - ok.
                            doUpdates(ignite, PESSIMISTIC, REPEATABLE_READ);

                            return null;
                        }
                    });

                svc.shutdown();
                svc.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

                IgniteCache<Long, Account> cache = ignite.cache(null);

                BigDecimal total = new BigDecimal(0);

                for (long i = 0; i < ACCOUNTS; i++) {
                    Account account = cache.get(i);

                    total = total.add(account.balance());
                }

                if (!total.equals(new BigDecimal(threadCnt * 10_000)))
                    System.out.println("Total sum check failed: " + total);
                else
                    System.out.println("Total sum check OK.");
            } finally {
                ignite.destroyCache(null);
            }
        }
    }

    /**
     * @param ignite Ignite instance.
     */
    private static void doUpdates(Ignite ignite, TransactionConcurrency conc, TransactionIsolation isol) {
        IgniteTransactions transactions = ignite.transactions();

        IgniteCache<Long, Account> cache = ignite.cache(null);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++) {
            while (true) {
                try (Transaction tx = transactions.txStart(conc, isol)) {
                    long accountId = rnd.nextInt(ACCOUNTS);

                    Account account = cache.get(accountId);

                    if (account == null)
                        account = new Account(accountId, new BigDecimal(1));
                    else
                        account = new Account(accountId, BigDecimal.ONE.add(account.balance()));

                    cache.put(accountId, account);

                    tx.commit();

                    break;
                }
                catch (TransactionOptimisticException ignore) {
                    // Will retry.
                }
            }
        }
    }

    /**
     * @param syncMode       Write synchronization mode.
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
