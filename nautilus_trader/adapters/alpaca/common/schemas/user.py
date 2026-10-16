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

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import msgspec

from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaOrder
from nautilus_trader.adapters.alpaca.common.schemas.account import iso_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


if TYPE_CHECKING:
    from nautilus_trader.adapters.alpaca.execution import AlpacaExecutionClient


class AlpacaTradeUpdateData(msgspec.Struct, frozen=True):
    """
    Payload of an Alpaca ``trade_updates`` stream message.

    References
    ----------
    https://docs.alpaca.markets/docs/websocket-streaming#trade-updates

    """

    event: str
    order: AlpacaOrder
    timestamp: str | None = None
    execution_id: str | None = None
    price: str | None = None
    qty: str | None = None
    position_qty: str | None = None

    def handle_trade_update(self, exec_client: AlpacaExecutionClient) -> None:  # noqa: C901 (too complex)
        """
        Generate the Nautilus order events for this update.
        """
        venue_order_id = VenueOrderId(self.order.id)
        client_order_id = exec_client._cache.client_order_id(venue_order_id) or ClientOrderId(
            self.order.client_order_id,
        )
        order = exec_client._cache.order(client_order_id)
        instrument_id = exec_client._get_cached_instrument_id(self.order.symbol)
        ts_event = iso_to_nanos(self.timestamp) or exec_client._clock.timestamp_ns()

        if order is None:
            # Not ours (or not cached yet): reconcile through a status report
            exec_client.create_task(exec_client._send_report_for(self.order))
            return

        strategy_id = order.strategy_id
        event = self.event

        if event in ("new", "accepted", "pending_new", "held", "accepted_for_bidding"):
            if order.status == OrderStatus.SUBMITTED:
                exec_client.generate_order_accepted(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)
        elif event in ("fill", "partial_fill"):
            instrument = exec_client._cache.instrument(instrument_id)
            if instrument is None or self.qty is None or self.price is None:
                exec_client.create_task(exec_client._send_report_for(self.order))
                return
            if order.status == OrderStatus.SUBMITTED:
                exec_client.generate_order_accepted(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)
            trade_id = TradeId(self.execution_id) if self.execution_id else TradeId(UUID4().value)
            exec_client.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=trade_id,
                order_side=order.side,
                order_type=order.order_type,
                last_qty=Quantity.from_str(str(int(Decimal(self.qty)))),
                last_px=Price(float(self.price), instrument.price_precision),
                quote_currency=USD,
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.MAKER if self.order.alpaca_type.value == "limit" else LiquiditySide.TAKER,
                ts_event=ts_event,
            )
            exec_client.create_task(exec_client._update_account_state())
        elif event in ("canceled", "done_for_day", "stopped", "suspended"):
            if order.is_open:
                exec_client.generate_order_canceled(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)
        elif event == "expired":
            if order.is_open:
                exec_client.generate_order_expired(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)
        elif event == "rejected":
            if order.status == OrderStatus.SUBMITTED:
                exec_client.generate_order_rejected(
                    strategy_id, instrument_id, client_order_id, "rejected by Alpaca", ts_event,
                )
            elif order.is_open:
                exec_client.generate_order_canceled(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)
        elif event == "replaced":
            pass  # the replacement's own events follow (OrderUpdated was generated on the replace response)
        elif event == "order_cancel_rejected":
            exec_client.generate_order_cancel_rejected(
                strategy_id, instrument_id, client_order_id, venue_order_id, "cancel rejected by Alpaca", ts_event,
            )
        elif event == "order_replace_rejected":
            exec_client.generate_order_modify_rejected(
                strategy_id, instrument_id, client_order_id, venue_order_id, "replace rejected by Alpaca", ts_event,
            )


class AlpacaTradeUpdate(msgspec.Struct, frozen=True):
    """
    An Alpaca ``trade_updates`` stream message.
    """

    stream: str
    data: AlpacaTradeUpdateData
