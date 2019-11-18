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
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class TwoStepSlaveProcessor implements EventProcessor {
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private final DataProvider<OrderCommand> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final WaitSpinningHelper waitSpinningHelper;
    private final SimpleEventHandler eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final ExceptionHandler<? super OrderCommand> exceptionHandler;
    private long nextSequence = -1;

    public TwoStepSlaveProcessor(final RingBuffer<OrderCommand> ringBuffer,
                                 final SequenceBarrier sequenceBarrier,
                                 final SimpleEventHandler eventHandler,
                                 final ExceptionHandler<? super OrderCommand> exceptionHandler) {
        this.dataProvider = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.waitSpinningHelper = new WaitSpinningHelper(ringBuffer, sequenceBarrier, 0, CoreWaitStrategy.NO_WAIT);
        this.eventHandler = eventHandler;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * 获取到{@link序列}参考正被此{@link EventProcessor}。
     *
     * @返回参照{@link序列}此{@link EventProcessor}
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
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run() {
        if (running.compareAndSet(IDLE, RUNNING)) {
            sequenceBarrier.clearAlert();
        } else if (running.get() == RUNNING) {
            //这是一些猜测工作。此时，运行状态可以更改为“已暂停”。但是，Java没有compareAndExchange，这是使其完全正确的唯一方法。
            throw new IllegalStateException("Thread is already running");
        }

        nextSequence = sequence.get() + 1L;
    }

    public void handlingCycle(long processUpToSequence) {
        while (true) {
            OrderCommand event = null;
            try {
                long availableSequence = waitSpinningHelper.tryWaitFor(nextSequence);

                // process batch
                while (nextSequence <= availableSequence && nextSequence < processUpToSequence) {
                    event = dataProvider.get(nextSequence);
                    eventHandler.onEvent(event);
                    nextSequence++;
                }

                // exit if finished processing entire group (up to specified sequence)
                if (nextSequence == processUpToSequence) {
                    sequence.set(nextSequence - 1);
                    return;
                }

                sequence.set(availableSequence);

            } catch (final Throwable ex) {
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }


}