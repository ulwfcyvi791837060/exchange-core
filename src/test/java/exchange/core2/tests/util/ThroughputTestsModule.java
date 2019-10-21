/*
 * Copyright 2019 Maksim Zheravin
 *
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
package exchange.core2.tests.util;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.ExchangeApi;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Slf4j
public class ThroughputTestsModule {


    /**
     * 吞吐量测试Impl
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 10:26
      * @param container 容器
     * @param totalTransactionsNumber 总交易数
     * @param targetOrderBookOrdersTotal 目标订单簿订单总数
     * @param numAccounts 数量帐户
     * @param iterations
     * @param currenciesAllowed 允许的货币
     * @param numSymbols
     * @param allowedSymbolTypes 允许的符号类型
     * @return void
     * @throws
     **/
    public static void throughputTestImpl(final ExchangeTestContainer container,
                                          final int totalTransactionsNumber,
                                          final int targetOrderBookOrdersTotal,
                                          final int numAccounts,
                                          final int iterations,
                                          final Set<Integer> currenciesAllowed,
                                          final int numSymbols,
                                          final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes) throws Exception {

        try (
                //假设我们现在有一个Java进程在运行，而我们希望将它绑定到某个特定的CPU上：
                //AffinityLock al = AffinityLock.acquireLock();
                final AffinityLock cpuLock = AffinityLock.acquireLock()) {

            //ExchangeTestContainer 构造方法里赋值
            final ExchangeApi api = container.api;

            //产生随机符号 一个符号
            final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            //产生用户 2K货币帐户
            final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

            //产生多个符号
            final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(
                    coreSymbolSpecifications,
                    totalTransactionsNumber,
                    usersAccounts,
                    //1K待定限价单
                    targetOrderBookOrdersTotal);

            List<Float> perfResults = new ArrayList<>();
            // iterations 多次取平均值
            for (int j = 0; j < iterations; j++) {
                //核心符号规格
                container.addSymbols(coreSymbolSpecifications);
                //预期全球余额 = 用户帐户初始化
                final IntLongHashMap globalBalancesExpected = container.userAccountsInit(usersAccounts);

                //断言
                assertThat(container.totalBalanceReport().getSum(), is(globalBalancesExpected));

                final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());
                container.setConsumer(cmd -> latchFill.countDown());
                //提交命令 api.submitCommand()
                genResult.getApiCommandsFill().forEach(api::submitCommand);
                latchFill.await();

                //闩锁基准
                final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());
                container.setConsumer(cmd -> latchBenchmark.countDown());
                long t = System.currentTimeMillis();
                genResult.getApiCommandsBenchmark().forEach(api::submitCommand);
                latchBenchmark.await();
                t = System.currentTimeMillis() - t;
                float perfMt = (float) genResult.getApiCommandsBenchmark().size() / (float) t / 1000.0f;
                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));
                perfResults.add(perfMt);

                assertThat(container.totalBalanceReport().getSum(), is(globalBalancesExpected));

                // 比较orderBook的最终状态只是为了确保所有命令都以相同的方式执行
                // compare orderBook final state just to make sure all commands executed same way
                // TODO 比较事件，余额，头寸
                // TODO compare events, balances, positions
                // 核心符号规格
                coreSymbolSpecifications.forEach(
                        symbol -> assertEquals(genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(),
                                // 请求当前订单
                                container.requestCurrentOrderBook(symbol.symbolId)));

                //重置Exchange Core
                container.resetExchangeCore();

                System.gc();
                Thread.sleep(300);
            }

            float avg = (float) perfResults.stream().mapToDouble(x -> x).average().orElse(0);
            // 20191020 在笔记本测试是 67647 次
            log.info("平均每秒多少次 M 是6个0 : {} MT/s", avg);
            log.info("平均每秒多少次 M 是6个0 : {} MT/s", avg);
            log.info("Average: {} MT/s", avg);
        }
    }

}
