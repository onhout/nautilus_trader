# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from decimal import Decimal

import msgspec
import pandas as pd

from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnumParser
from nautilus_trader.adapters.alpaca.common.enums import AlpacaOrderSide
from nautilus_trader.adapters.alpaca.common.enums import AlpacaOrderStatus
from nautilus_trader.adapters.alpaca.common.enums import AlpacaOrderType
from nautilus_trader.adapters.alpaca.common.enums import AlpacaTimeInForce
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def iso_to_nanos(value: str | None) -> int | None:
    return pd.Timestamp(value).value if value else None


class AlpacaAccount(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/account`.
    """

    id: str
    account_number: str
    status: str
    currency: str = "USD"
    cash: str = "0"
    equity: str | None = None
    portfolio_value: str | None = None
    buying_power: str | None = None
    non_marginable_buying_power: str | None = None
    options_buying_power: str | None = None
    options_approved_level: int | None = None
    options_trading_level: int | None = None
    multiplier: str | None = None
    pattern_day_trader: bool | None = None
    trading_blocked: bool | None = None
    account_blocked: bool | None = None
    shorting_enabled: bool | None = None

    def balances(self) -> tuple[Decimal, Decimal]:
        """
        Return (total, free) cash for a CASH account view: free is what can open options or
        unleveraged stock positions.
        """
        cash = Decimal(self.cash or 0)
        free = Decimal(
            self.options_buying_power or self.non_marginable_buying_power or self.buying_power or 0,
        )
        total = max(cash, free, Decimal(0))
        return total, min(max(free, Decimal(0)), total)


class AlpacaClock(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/clock`.
    """

    timestamp: str
    is_open: bool
    next_open: str
    next_close: str


class AlpacaPosition(msgspec.Struct, frozen=True):
    """
    HTTP response item from Alpaca `GET /v2/positions`.
    """

    asset_id: str
    symbol: str
    asset_class: str
    qty: str
    side: str
    avg_entry_price: str | None = None
    qty_available: str | None = None
    market_value: str | None = None
    cost_basis: str | None = None
    unrealized_pl: str | None = None
    current_price: str | None = None

    def parse_to_position_status_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> PositionStatusReport:
        quantity = abs(Decimal(self.qty))
        if quantity == 0:
            side = PositionSide.FLAT
        else:
            side = PositionSide.SHORT if self.side == "short" or Decimal(self.qty) < 0 else PositionSide.LONG
        return PositionStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            position_side=side,
            quantity=Quantity.from_str(str(int(quantity))),
            avg_px_open=Decimal(self.avg_entry_price) if self.avg_entry_price else None,
            report_id=UUID4(),
            ts_last=ts_init,
            ts_init=ts_init,
        )


class AlpacaOrder(msgspec.Struct, frozen=True):
    """
    HTTP response from Alpaca `GET /v2/orders/{order_id}` (and the order of a trade update).
    """

    id: str
    client_order_id: str
    symbol: str
    side: AlpacaOrderSide
    status: AlpacaOrderStatus
    created_at: str
    updated_at: str | None = None
    submitted_at: str | None = None
    filled_at: str | None = None
    expired_at: str | None = None
    canceled_at: str | None = None
    failed_at: str | None = None
    replaced_at: str | None = None
    replaced_by: str | None = None
    replaces: str | None = None
    asset_id: str | None = None
    asset_class: str | None = None
    notional: str | None = None
    qty: str | None = None
    filled_qty: str = "0"
    filled_avg_price: str | None = None
    order_class: str | None = None
    type: AlpacaOrderType | None = None
    order_type: AlpacaOrderType | None = None
    position_intent: str | None = None
    time_in_force: AlpacaTimeInForce = AlpacaTimeInForce.DAY
    limit_price: str | None = None
    stop_price: str | None = None
    extended_hours: bool | None = None
    trail_percent: str | None = None
    trail_price: str | None = None
    hwm: str | None = None

    @property
    def alpaca_type(self) -> AlpacaOrderType:
        return self.type or self.order_type or AlpacaOrderType.MARKET

    @property
    def is_open(self) -> bool:
        return self.status not in (
            AlpacaOrderStatus.FILLED,
            AlpacaOrderStatus.CANCELED,
            AlpacaOrderStatus.EXPIRED,
            AlpacaOrderStatus.REJECTED,
            AlpacaOrderStatus.REPLACED,
            AlpacaOrderStatus.DONE_FOR_DAY,
        )

    def parse_to_order_status_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: int,
        client_order_id: ClientOrderId | None,
        ts_init: int,
        enum_parser: AlpacaEnumParser,
    ) -> OrderStatusReport:
        filled = Decimal(self.filled_qty or 0)
        status = enum_parser.parse_alpaca_order_status(self.status)
        if status == OrderStatus.ACCEPTED and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        if status == OrderStatus.REJECTED and filled > 0:
            status = OrderStatus.CANCELED  # Nautilus can't reject a partially filled order
        order_type = enum_parser.parse_alpaca_order_type(self.alpaca_type)
        trailing: dict = {}
        if order_type == OrderType.TRAILING_STOP_MARKET:
            if self.trail_percent is not None:
                trailing = {
                    "trailing_offset": Decimal(self.trail_percent) * 100,
                    "trailing_offset_type": TrailingOffsetType.BASIS_POINTS,
                }
            else:
                trailing = {
                    "trailing_offset": Decimal(self.trail_price or 0),
                    "trailing_offset_type": TrailingOffsetType.PRICE,
                }
        ts_accepted = iso_to_nanos(self.submitted_at or self.created_at) or ts_init
        return OrderStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=VenueOrderId(self.id),
            order_side=enum_parser.parse_alpaca_order_side(self.side),
            order_type=order_type,
            time_in_force=enum_parser.parse_alpaca_time_in_force(self.time_in_force),
            order_status=status,
            quantity=Quantity.from_str(str(int(Decimal(self.qty or self.filled_qty or 0)))),
            filled_qty=Quantity.from_str(str(int(filled))),
            price=Price(float(self.limit_price), price_precision) if self.limit_price else None,
            trigger_price=Price(float(self.stop_price), price_precision) if self.stop_price else None,
            trigger_type=TriggerType.DEFAULT if self.stop_price or trailing else TriggerType.NO_TRIGGER,
            avg_px=Decimal(self.filled_avg_price) if self.filled_avg_price else None,
            report_id=UUID4(),
            ts_accepted=ts_accepted,
            ts_last=iso_to_nanos(self.updated_at) or ts_accepted,
            ts_init=ts_init,
            **trailing,
        )


class AlpacaFillActivity(msgspec.Struct, frozen=True):
    """
    HTTP response item from Alpaca `GET /v2/account/activities/FILL`.
    """

    id: str
    activity_type: str
    transaction_time: str
    type: str
    price: str
    qty: str
    side: str
    symbol: str
    order_id: str
    leaves_qty: str | None = None
    cum_qty: str | None = None
    order_status: str | None = None

    def parse_to_fill_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        price_precision: int,
        client_order_id: ClientOrderId | None,
        trade_id: TradeId,
        liquidity_side: LiquiditySide,
        enum_parser: AlpacaEnumParser,
        ts_init: int,
    ) -> FillReport:
        side = AlpacaOrderSide.BUY if self.side.startswith("buy") else AlpacaOrderSide.SELL
        return FillReport(
            account_id=account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(self.order_id),
            client_order_id=client_order_id,
            trade_id=trade_id,
            order_side=enum_parser.parse_alpaca_order_side(side),
            last_qty=Quantity.from_str(str(int(Decimal(self.qty)))),
            last_px=Price(float(self.price), price_precision),
            commission=Money(0, USD),
            liquidity_side=liquidity_side,
            report_id=UUID4(),
            ts_event=iso_to_nanos(self.transaction_time) or ts_init,
            ts_init=ts_init,
        )
