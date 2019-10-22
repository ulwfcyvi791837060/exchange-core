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
package exchange.core2.core.common;


import lombok.*;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

@Builder
@AllArgsConstructor
@Getter
@ToString
public final class CoreSymbolSpecification implements WriteBytesMarshallable, StateHash {

    public final int symbolId;

    @NonNull
    public final SymbolType type;

    // currency pair specification
    //基本货币
    public final int baseCurrency;  // base currency
    // 报价/柜台货币（或期货合约货币）
    public final int quoteCurrency; // quote/counter currency (OR futures contract currency)
    //基本货币数量乘数（以基本货币为单位的手数）
    public final long baseScaleK;   // base currency amount multiplier (lot size in base currency units)
    //报价货币金额乘数（步长以报价货币为单位）
    public final long quoteScaleK;  // quote currency amount multiplier (step size in quote currency units)

    // fees per lot in quote? currency units 报价每手收费？货币单位
    public final long takerFee; // TODO check invariant: taker fee is not less than maker fee 不变支票：收取者的费用不少于制造者的费用
    public final long makerFee;

    // margin settings (for type=FUTURES_CONTRACT only) 边距设置（仅对于类型= FUTURES_CONTRACT）
    public final long marginBuy;   // buy margin (quote currency) 购买保证金（报价货币）
    public final long marginSell;  // sell margin (quote currency) 卖出保证金（报价货币）

    //比Java NIO的ByteBuffer性能更快的Chronicle-Bytes?
    public CoreSymbolSpecification(BytesIn bytes) {
        this.symbolId = bytes.readInt();
        this.type = SymbolType.of(bytes.readByte());
        this.baseCurrency = bytes.readInt();
        this.quoteCurrency = bytes.readInt();
        this.baseScaleK = bytes.readLong();
        this.quoteScaleK = bytes.readLong();
        this.takerFee = bytes.readLong();
        this.makerFee = bytes.readLong();
        this.marginBuy = bytes.readLong();
        this.marginSell = bytes.readLong();
    }

/* NOT SUPPORTED YET: 尚未支持

//    order book limits -- for FUTURES only 订单限制-仅适用于期货
//    public final long highLimit; 高限
//    public final long lowLimit;

//    swaps -- not by
//    public final long longSwap; 长期互换
//    public final long shortSwap;

// activity (inactive, active, expired)

  */

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbolId);
        bytes.writeByte(type.getCode());
        bytes.writeInt(baseCurrency);
        bytes.writeInt(quoteCurrency);
        bytes.writeLong(baseScaleK);
        bytes.writeLong(quoteScaleK);
        bytes.writeLong(takerFee);
        bytes.writeLong(makerFee);
        bytes.writeLong(marginBuy);
        bytes.writeLong(marginSell);
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                symbolId,
                type.getCode(),
                baseCurrency,
                quoteCurrency,
                baseScaleK,
                quoteScaleK,
                takerFee,
                makerFee,
                marginBuy,
                marginSell);
    }
}
