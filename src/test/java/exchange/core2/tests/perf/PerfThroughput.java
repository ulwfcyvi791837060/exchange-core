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
package exchange.core2.tests.perf;

import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.ThroughputTestsModule;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfThroughput {

    // TODO 如果测试失败，则关闭中断器
    // TODO shutdown disruptor if test fails

    /**
     * 延迟测试：mvn -Dtest = PerfLatency＃latencyTest测试
     * 吞吐量测试：mvn -Dtest = PerfThroughput＃throughputTest测试
     * 打ic测试：mvn -Dtest = PerfHiccups＃hiccupsTest测试
     */

    /**
     * 这是针对简化条件的吞吐量测试
     *       *-一个符号
     *       *-约1K活跃用户（2K货币帐户）
     *       *-1K待定限价单（在一本订单簿中）
     *       * 6线程CPU可以运行此测试
     *
     小数点后移 6
     5.000000
     0.06764774 MT/s = 67647.74 MT/s
     *
     * This is throughput test for simplified conditions
     * - one symbol
     * - ~1K active users (2K currency accounts)
     * - 1K pending limit-orders (in one order book)
     * 6-threads CPU can run this test 6线程CPU可以运行此测试
     */

    /**
     * 测试吞吐量保证金 6线程CPU可以运行此测试
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 10:23
      * @param
     * @return void
     * @throws
     **/
    @Test
    public void testThroughputMargin() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {

            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000, //原来 3_000_000 , java.lang.OutOfMemoryError
                    //1K待定限价单
                    1000,
                    //2K货币帐户
                    2000,
                    //迭代
                    50,
                    //货币期货
                    TestConstants.CURRENCIES_FUTURES,
                    1,
                    //期货合约
                    ExchangeTestContainer.AllowedSymbolTypes.FUTURES_CONTRACT);
        }
    }

    /**
     * 测试吞吐量交换
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/22 12:24
      * @param
     * @return void
     * @throws
     **/
    @Test
    public void testThroughputExchange() throws Exception {
        //stateId = null 不从持久化状态加载
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1,
                1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000,//3_000_000,
                    1000,
                    2000,
                    50,
                    //货币兑换
                    TestConstants.CURRENCIES_EXCHANGE,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
        }
    }

    /**
     * This is high load throughput test for verifying "triple million" capability:
     * - 10M currency accounts (~3M active users)
     * - 1M pending limit-orders (in 1K order books)
     * - 1K symbols
     * - at least 1M messages per second throughput
     * 12-threads CPU and 32GiB RAM is required for running this test in 4+4 configuration. 在4 + 4配置中运行此测试需要12线程CPU和32GiB RAM。
     */
    /**
     * 测试吞吐量多符号
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/22 12:25
      * @param
     * @return void
     * @throws
     **/
    @Test
    public void testThroughputMultiSymbol() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(64 * 1024, 4, 4, 2048, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    5_000_000,
                    1_000_000,
                    10_000_000,
                    25,
                    TestConstants.ALL_CURRENCIES,
                    1_000,
                    ExchangeTestContainer.AllowedSymbolTypes.BOTH);
        }
    }

}