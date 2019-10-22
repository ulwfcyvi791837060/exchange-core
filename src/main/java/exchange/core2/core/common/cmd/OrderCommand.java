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
package exchange.core2.core.common.cmd;

import com.google.common.collect.Lists;
import exchange.core2.core.common.*;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public final class OrderCommand implements IOrder {

    public OrderCommandType command;

    @Getter
    public long orderId;

    public int symbol;

    @Getter
    public long price;

    @Getter
    public long size;

    @Getter
    // new orders - reserved price for fast moves of GTC bid orders in exchange mode 新订单-在交换模式下GTC投标订单快速移动的保留价格
    public long reserveBidPrice;

    // required for PLACE_ORDER only; 仅对于PLACE_ORDER是必需的；
    @Getter
    public OrderAction action;

    public OrderType orderType;

    @Getter
    public long uid;

    @Getter
    public long timestamp;

    public int userCookie;

    // filled by grouping processor: 由分组处理器填充

    public long eventsGroup;
    public int serviceFlags;

    // result code of command execution - can also be used for saving intermediate state 命令执行的结果代码-也可以用于保存中间状态
    public CommandResultCode resultCode;

    // trade events chain 贸易事件链
    public MatcherTradeEvent matcherEvent;

    // optional market data 可选市场数据
    public L2MarketData marketData;

    // sequence of last available for this command 该命令最后可用的顺序
    //public long matcherEventSequence; 匹配器事件序列
    // ---- potential false sharing section ------ 潜在的虚假分享部分

    public static OrderCommand newOrder(OrderType orderType, long orderId, int uid, long price, long reserveBidPrice, long size, OrderAction action) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.reserveBidPrice = reserveBidPrice;
        cmd.size = size;
        cmd.action = action;
        cmd.orderType = orderType;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand cancel(long orderId, int uid) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    public static OrderCommand update(long orderId, int uid, long price) {
        OrderCommand cmd = new OrderCommand();
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.orderId = orderId;
        cmd.uid = uid;
        cmd.price = price;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        return cmd;
    }

    /**
     * Handles full MatcherTradeEvent chain, without removing/revoking them 处理完整的MatcherTradeEvent链，而无需删除/撤销它们
     *
     * @param handler - MatcherTradeEvent handler
     */
    public void processMatcherEvents(Consumer<MatcherTradeEvent> handler) {
        MatcherTradeEvent mte = this.matcherEvent;
        while (mte != null) {
            handler.accept(mte);
            mte = mte.nextEvent;
        }
    }

    /**
     * Produces garbage
     * For testing only !!! 产生垃圾仅用于测试
     *
     * @return
     */
    public List<MatcherTradeEvent> extractEvents() {
        List<MatcherTradeEvent> list = new ArrayList<>();
        processMatcherEvents(list::add);
        return Lists.reverse(list);
    }

    // Traverse and remove:
//    private void cleanMatcherEvents() {
//        MatcherTradeEvent ev = this.matcherEvent;
//        this.matcherEvent = null;
//        while (ev != null) {
//            MatcherTradeEvent tmp = ev;
//            ev = ev.nextEvent;
//            tmp.nextEvent = null;
//        }
//    }
//


    /**
     * Write only command data, not status or events 只写命令数据，不写状态或事件
     *
     * @param cmd2
     */
    public void writeTo(OrderCommand cmd2) {
        cmd2.command = this.command;
        cmd2.orderId = this.orderId;
        cmd2.symbol = this.symbol;
        cmd2.uid = this.uid;
        cmd2.timestamp = this.timestamp;

        cmd2.reserveBidPrice = this.reserveBidPrice;
        cmd2.price = this.price;
        cmd2.size = this.size;
        cmd2.action = this.action;
        cmd2.orderType = this.orderType;
    }

    public OrderCommand copy() {

        OrderCommand newCmd = new OrderCommand();
        writeTo(newCmd);
        newCmd.resultCode = this.resultCode;

        List<MatcherTradeEvent> events = extractEvents();

//        System.out.println(">>> events: " + events);
        for (MatcherTradeEvent event : events) {
            MatcherTradeEvent copy = event.copy();
            copy.nextEvent = newCmd.matcherEvent;
            newCmd.matcherEvent = copy;
//            System.out.println(">>> 新的Cmd.matcher事件： newCmd.matcherEvent: " + newCmd.matcherEvent);
        }

        if (marketData != null) {
            newCmd.marketData = marketData.copy();
        }

//        System.out.println(">>> newCmd: " + newCmd);
        return newCmd;
    }

}
