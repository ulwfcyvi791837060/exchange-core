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
package exchange.core2.core;

import com.google.common.collect.Streams;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core2.core.processors.GroupingProcessor;
import exchange.core2.core.processors.TwoStepMasterProcessor;
import exchange.core2.core.processors.TwoStepSlaveProcessor;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.processors.DisruptorExceptionHandler;
import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.RiskEngine;
import exchange.core2.core.processors.journalling.ISerializationProcessor;
import exchange.core2.core.processors.journalling.JournallingProcessor;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final ExchangeApi api;

    // core can be started and stopped only once
    private boolean started = false;
    private boolean stopped = false;

    @Builder
    public ExchangeCore(final ObjLongConsumer<OrderCommand> resultsConsumer,
                        final JournallingProcessor journallingHandler,
                        final ISerializationProcessor serializationProcessor,
                        final int ringBufferSize,
                        final int matchingEnginesNum,
                        final int riskEnginesNum,
                        final int msgsInGroupLimit,
                        final UnsafeUtils.ThreadAffinityMode threadAffinityMode,
                        final CoreWaitStrategy waitStrategy,
                        final Function<CoreSymbolSpecification, IOrderBook> orderBookFactory,
                        final Long loadStateId) {

        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                UnsafeUtils.affinedThreadFactory(threadAffinityMode),
                //多个网关线程正在写入
                ProducerType.MULTI, // multiple gateway threads are writing
                waitStrategy.create());

        this.api = new ExchangeApi(disruptor.getRingBuffer());

        // 创建和附加异常处理程序
        // creating and attaching exceptions handler
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO re-throw exception on publishing
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);

        // 定义撮合引擎
        // creating matching engine event handlers array // TODO parallel deserialization 并行反序列化
        final EventHandler<OrderCommand>[] matchingEngineHandlers = IntStream.range(0, matchingEnginesNum)
                .mapToObj(shardId -> {
                    final MatchingEngineRouter router = new MatchingEngineRouter(shardId, matchingEnginesNum, serializationProcessor, orderBookFactory, loadStateId);
                    // 应该只是定义,在需要的时候才会运行
                    return (EventHandler<OrderCommand>) (cmd, seq, eob) -> router.processOrder(cmd);
                })
                .toArray(ExchangeCore::newEventHandlersArray);

        // 定义风险引擎数组
        // creating risk engines array // TODO parallel deserialization 并行反序列化
        final List<RiskEngine> riskEngines = IntStream.range(0, riskEnginesNum)
                .mapToObj(shardId -> new RiskEngine(shardId, riskEnginesNum, serializationProcessor, loadStateId))
                .collect(Collectors.toList());

        final List<TwoStepMasterProcessor> procR1 = new ArrayList<>(riskEnginesNum);
        final List<TwoStepSlaveProcessor> procR2 = new ArrayList<>(riskEnginesNum);

        // 定义分组处理器 顺序执行
        // 1. grouping processor (G)
        final EventHandlerGroup<OrderCommand> afterGrouping =
                disruptor.handleEventsWith((rb, bs) -> new GroupingProcessor(rb, rb.newBarrier(bs), msgsInGroupLimit, waitStrategy));


        // [日志（J）]与风险保留（R1）+匹配引擎（ME）并行 start
        // 2. [journalling (J)] in parallel with risk hold (R1) + matching engine (ME)
        if (journallingHandler != null) {
            //afterGrouping后顺序执行
            afterGrouping.handleEventsWith(journallingHandler::onEvent);
        }

        //分组处理器 后 风险引擎们与日志并行处理
        //afterGrouping 后顺序执行
        riskEngines.forEach(riskEngine -> afterGrouping.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepMasterProcessor r1 = new TwoStepMasterProcessor(rb, rb.newBarrier(bs), riskEngine::preProcessCommand, exceptionHandler, waitStrategy);
                    procR1.add(r1);
                    return r1;
                }));

        // 风险引擎们 riskEngines 后 撮合再执行
        // 返回一个数组，其中包含此列表中的所有元素
        disruptor.after(procR1.toArray(new TwoStepMasterProcessor[0])).handleEventsWith(matchingEngineHandlers);

        // [日志（J）]与风险保留（R1）+匹配引擎（ME）并行 end ?

        // 在撮合后风险释放再执行
        // 匹配引擎（ME）后的风险释放（R2）
        // 3. risk release (R2) after matching engine (ME)
        final EventHandlerGroup<OrderCommand> afterMatchingEngine = disruptor.after(matchingEngineHandlers);

        riskEngines.forEach(riskEngine -> afterMatchingEngine.handleEventsWith(
                (rb, bs) -> {
                    final TwoStepSlaveProcessor r2 = new TwoStepSlaveProcessor(rb, rb.newBarrier(bs), riskEngine::handlerRiskRelease, exceptionHandler);
                    procR2.add(r2);
                    return r2;
                }));

        /**
         *          //1,2,last顺序执行
         *         //disruptor.handleEventsWith(new LongEventHandler()).handleEventsWith(new SecondEventHandler())
         *         //        .handleEventsWith(new LastEventHandler());
         *
         *         //也是1，2，last顺序执行
         *         //disruptor.handleEventsWith(firstEventHandler);
         *         //disruptor.after(firstEventHandler).handleEventsWith(secondHandler).then(lastEventHandler);
         *
         *         //1,2并发执行，之后才是last
         *         //disruptor.handleEventsWith(firstEventHandler, secondHandler);
         *         //disruptor.after(firstEventHandler, secondHandler).handleEventsWith(lastEventHandler);
         *
         *  //1后2，3后4，1和3并发，2和4都结束后last
         *         disruptor.handleEventsWith(firstEventHandler, thirdEventHandler);
         *         disruptor.after(firstEventHandler).handleEventsWith(secondHandler);
         *         disruptor.after(thirdEventHandler).handleEventsWith(fourthEventHandler);
         *         disruptor.after(secondHandler, fourthEventHandler).handleEventsWith(lastEventHandler);
         * ————————————————
         * 版权声明：本文为CSDN博主「天涯泪小武」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
         * 原文链接：https://blog.csdn.net/tianyaleixiaowu/article/details/79787377
         */

        // journallingHandler::onEvent 访问 onEvent 方法
        // 结果处理程序在撮合和日志(可选)后面 撮合和日志并行
        // 匹配引擎（ME）+ [新闻（J）]之后的结果处理程序（E）
        // 4. results handler (E) after matching engine (ME) + [journalling (J)]
        (journallingHandler != null ? disruptor.after(ArrayUtils.add(matchingEngineHandlers, journallingHandler::onEvent)) : afterMatchingEngine)
                .handleEventsWith((cmd, seq, eob) -> {
                    resultsConsumer.accept(cmd, seq);
                    // TODO 待办事项缓慢？（易失性操作）
                    // TODO SLOW ?(volatile operations)
                    api.processResult(seq, cmd);
                });

        // 将从处理器连接到主处理器
        // attach slave processors to master processor
        Streams.forEachPair(procR1.stream(), procR2.stream(), TwoStepMasterProcessor::setSlaveProcessor);

    }

    public synchronized void startup() {
        if (!started) {
            log.debug("Starting disruptor...");
            disruptor.start();
            started = true;
        }
    }

    public ExchangeApi getApi() {
        return api;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL;
        cmd.resultCode = CommandResultCode.NEW;
    };

    public synchronized void shutdown() {
        if (!stopped) {
            stopped = true;
            // TODO stop accepting new events first
            log.info("Shutdown disruptor...");
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
            log.info("Disruptor stopped");
        }
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }
}
