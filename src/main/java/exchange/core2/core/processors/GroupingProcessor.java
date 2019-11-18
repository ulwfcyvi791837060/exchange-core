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
package exchange.core2.core.processors;

import com.lmax.disruptor.*;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 以前的Customer（消费者），现在叫EventProcessor（事件处理器）和EventHandler（事件句柄）
 * @Author zenghuikang
 * @Description
 * @Date 2019/10/23 19:04
  * @param null
 * @return
 * @throws
 **/
@Slf4j
public final class GroupingProcessor implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private static final int GROUP_SPIN_LIMIT = 1000;

    private static final int L2_PUBLISH_INTERVAL_NS = 10_000_000;
    private static final int GROUP_MAX_DURATION_NS = 10_000;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final RingBuffer<OrderCommand> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final WaitSpinningHelper waitSpinningHelper;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    private final long msgsInGroupLimit;

    public GroupingProcessor(RingBuffer<OrderCommand> ringBuffer, SequenceBarrier sequenceBarrier, long msgsInGroupLimit, CoreWaitStrategy coreWaitStrategy) {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, GROUP_SPIN_LIMIT, coreWaitStrategy);
        this.msgsInGroupLimit = msgsInGroupLimit;
    }

    /**
     * 获取到{@link序列}参考正被此{@link EventProcessor}。
     *
     * @返回参照 {@link序列}此{@link EventProcessor}
     **/
    @Override
    public Sequence getSequence() {
        return sequence;
    }

    /**
     * 信号，当它已经完成了在下一一刀两断消费这EventProcessor应该停止。
     * 它会调用{@link SequenceBarrier＃警报（）}通知线程检查状态。
     **/
    @Override
    public void halt() {
        running.set(HALTED);
        //Barrier调用alert方法，在后面消费者消费的时候，会查看该状态，以决定是否阻塞在ringBuffer上等待事件。
        //通知消费者状态变化。当调用EventProcessor#halt()将调用此方法。
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    } //闲


    /**
     * 可以在 停止() halt（）之后让另一个线程重新运行此方法。
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread 如果此对象实例已在线程中运行
     */
    @Override
    public void run() {
        /**
         如果当前值{@code ==}是期望值，则以原子方式将该值设置为给定的更新值。
         **/
        if (running.compareAndSet(IDLE, RUNNING)) {
            //清除当前警报状态
            sequenceBarrier.clearAlert();
            try {
                if (running.get() == RUNNING) {
                    processEvents();
                }
            } finally {
                running.set(IDLE);
            }
        } else {
            /**
             这是一些猜测工作。此时，运行状态可以更改为“已暂停”。但是，Java没有compareAndExchange，这是使其完全正确的唯一方法。
             **/
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running 线程已经在运行");
            }
        }
    }

    private void processEvents() {
        long nextSequence = sequence.get() + 1L;

        long groupCounter = 0;
        long msgsInGroup = 0;

        long groupLastNs = 0;

        long l2dataLastNs = 0;
        boolean triggerL2DataRequest = false;

        while (true) {
            try {

                // 应该旋转并检查另一个障碍
                // should spin and also check another barrier
                // 等待旋转助手
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                if (nextSequence <= availableSequence) {
                    while (nextSequence <= availableSequence) {

                        OrderCommand cmd = ringBuffer.get(nextSequence);
                        nextSequence++;

                        // 一些命令应触发R2阶段以避免事件中未处理的状态
                        // some commands should trigger R2 stage to avoid unprocessed state in events
                        if (cmd.command == OrderCommandType.RESET
                                || cmd.command == OrderCommandType.PERSIST_STATE_MATCHING
                                || cmd.command == OrderCommandType.BINARY_DATA) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                        cmd.eventsGroup = groupCounter;

                        cmd.serviceFlags = 0;
                        if (triggerL2DataRequest) {
                            triggerL2DataRequest = false;
                            cmd.serviceFlags = 1;
                        }

                        // 清洁附着的物体
                        // cleaning attached objects
                        cmd.marketData = null;
                        cmd.matcherEvent = null;

                        if (cmd.command == OrderCommandType.NOP) {
                            // 只需设置下一组并通过
                            // just set next group and pass
                            continue;
                        }

                        msgsInGroup++;

                        // switch group after each N messages 每N条消息后切换组
                        // avoid changing groups when PERSIST_STATE_MATCHING is already executing
                        // 当已经执行PERSIST_STATE_MATCHING时避免更改组
                        if (msgsInGroup >= msgsInGroupLimit && cmd.command != OrderCommandType.PERSIST_STATE_RISK) {
                            groupCounter++;
                            msgsInGroup = 0;
                        }

                    }
                    sequence.set(availableSequence);
                    groupLastNs = System.nanoTime() + GROUP_MAX_DURATION_NS;

                } else {
                    final long t = System.nanoTime();
                    if (msgsInGroup > 0 && t > groupLastNs) {
                        // switch group after T microseconds elapsed, if group is non empty
                        groupCounter++;
                        msgsInGroup = 0;
                    }

                    if (t > l2dataLastNs) {
                        l2dataLastNs = t + L2_PUBLISH_INTERVAL_NS; // trigger L2 data every 10ms
                        triggerL2DataRequest = true;
                    }
                }

            } catch (final AlertException ex) {
                if (running.get() != RUNNING) {
                    break;
                }
            } catch (final Throwable ex) {
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }
}