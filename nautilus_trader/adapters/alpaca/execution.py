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

import asyncio
from decimal import Decimal
from typing import Any

import msgspec
import pandas as pd

from nautilus_trader.adapters.alpaca.common.constants import ALPACA_VENUE
from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnumParser
from nautilus_trader.adapters.alpaca.common.enums import AlpacaEnvironment
from nautilus_trader.adapters.alpaca.common.enums import AlpacaPositionIntent
from nautilus_trader.adapters.alpaca.common.schemas.account import AlpacaOrder
from nautilus_trader.adapters.alpaca.common.schemas.user import AlpacaTradeUpdate
from nautilus_trader.adapters.alpaca.common.symbol import alpaca_symbol_to_instrument_id
from nautilus_trader.adapters.alpaca.common.symbol import is_option_symbol
from nautilus_trader.adapters.alpaca.common.urls import get_ws_trading_url
from nautilus_trader.adapters.alpaca.config import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.http.account import AlpacaAccountHttpAPI
from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.http.error import AlpacaError
from nautilus_trader.adapters.alpaca.http.error import should_retry
from nautilus_trader.adapters.alpaca.providers import AlpacaInstrumentProvider
from nautilus_trader.adapters.alpaca.websocket.client import AlpacaWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.retry import RetryManagerPool
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order


class AlpacaExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for Alpaca (US equities and options), paper or live.

    Orders go through the REST API; their events come from the account's ``trade_updates``
    stream. Missed events are recovered by the engine's reconciliation (status, fill and position
    reports from REST).

    Supported orders: market, limit, stop, stop-limit, and trailing stops for stocks (Alpaca
    has no trailing stops for options: submit those with an ``emulation_trigger`` so Nautilus
    emulates them). Options: BUY opens (or closes a short), SELL closes a long; selling options
    to open is denied.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : AlpacaHttpClient
        The Alpaca HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : AlpacaInstrumentProvider
        The instrument provider.
    config : AlpacaExecClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaExecClientConfig,
        name: str | None = None,
    ) -> None:
        if config.environment == AlpacaEnvironment.LIVE and not config.live_trading:
            raise ValueError("The LIVE Alpaca account needs live_trading=True")
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        self._config = config
        self._http_account = AlpacaAccountHttpAPI(client)
        self._enum_parser = AlpacaEnumParser()
        self._ws_client = AlpacaWebSocketClient(
            clock=clock,
            base_url=get_ws_trading_url(config.environment),
            api_key=client.api_key,
            api_secret=client.api_secret,
            handler=self._handle_user_ws_message,
            loop=loop,
            proxy_url=config.proxy_url,
        )
        self._retry_manager_pool = RetryManagerPool[None](
            pool_size=50,
            max_retries=config.max_retries,
            delay_initial_ms=500,
            delay_max_ms=5000,
            backoff_factor=2,
            logger=self._log,
            exc_types=(AlpacaError,),
            retry_check=should_retry,
        )
        # The stream can confirm an order before its REST response returns: accept each order once.
        self._accepted: set[ClientOrderId] = set()
        kind = "PAPER account (simulated money)" if config.environment == AlpacaEnvironment.PAPER else "LIVE account (REAL MONEY)"
        self._log.warning(f"Alpaca execution: {kind}", LogColor.YELLOW)

    @property
    def instrument_provider(self) -> AlpacaInstrumentProvider:
        return self._instrument_provider

    # -- CONNECTION -------------------------------------------------------------------------------

    async def _connect(self) -> None:
        account = await self._http_account.query_account()
        self._set_account_id(AccountId(f"{ALPACA_VENUE.value}-{account.account_number[-4:]}"))
        self._log.info(
            f"Account ••••{account.account_number[-4:]} {account.status}, options level {account.options_trading_level}",
            LogColor.BLUE,
        )
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._cache.add_instrument(instrument)
        await self._update_account_state()
        await self._await_account_registered()
        await self._ws_client.listen(["trade_updates"])
        await self._ws_client.connect()

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    def _stop(self) -> None:
        self._retry_manager_pool.shutdown()

    # -- ACCOUNT ----------------------------------------------------------------------------------

    async def _update_account_state(self) -> None:
        account = await self._http_account.query_account()
        total, free = account.balances()
        self.generate_account_state(
            balances=[AccountBalance(total=Money(total, USD), locked=Money(total - free, USD), free=Money(free, USD))],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _query_account(self, command: QueryAccount) -> None:
        await self._update_account_state()

    # -- HELPERS ----------------------------------------------------------------------------------

    def _get_cached_instrument_id(self, symbol: str) -> InstrumentId:
        return alpaca_symbol_to_instrument_id(symbol)

    async def _instrument_for(self, symbol: str):
        instrument_id = alpaca_symbol_to_instrument_id(symbol)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
            if instrument is not None:
                self._cache.add_instrument(instrument)
        return instrument

    def _net_position(self, instrument_id: InstrumentId) -> Decimal:
        net = Decimal(0)
        for position in self._cache.positions_open(instrument_id=instrument_id):
            qty = position.quantity.as_decimal()
            net += qty if position.is_long else -qty
        return net

    def _accept(self, strategy_id, instrument_id, client_order_id: ClientOrderId, venue_order_id: VenueOrderId, ts_event: int) -> None:
        if client_order_id in self._accepted:
            return
        order = self._cache.order(client_order_id)
        if order is not None and order.status != OrderStatus.SUBMITTED:
            return
        self._accepted.add(client_order_id)
        self.generate_order_accepted(strategy_id, instrument_id, client_order_id, venue_order_id, ts_event)

    def _client_order_id_for(self, order: AlpacaOrder) -> ClientOrderId:
        return self._cache.client_order_id(VenueOrderId(order.id)) or ClientOrderId(order.client_order_id)

    async def _send_report_for(self, order: AlpacaOrder) -> None:
        report = await self._order_report(order)
        if report is not None:
            self._send_order_status_report(report)

    # -- COMMANDS ---------------------------------------------------------------------------------

    def _validate_order_pre_submit(self, order: Order) -> str | None:
        if self._enum_parser.parse_internal_order_type(order.order_type) is None:
            return f"UNSUPPORTED_ORDER_TYPE: {order.order_type.name}"
        if self._enum_parser.parse_internal_time_in_force(order.time_in_force) is None:
            return f"UNSUPPORTED_TIME_IN_FORCE: {order.time_in_force.name}"
        if order.quantity.as_decimal() != order.quantity.as_decimal().to_integral_value():
            return "FRACTIONAL_QUANTITY: whole shares and contracts only"
        if is_option_symbol(order.instrument_id.symbol.value):
            if order.order_type == OrderType.TRAILING_STOP_MARKET:
                return "UNSUPPORTED_ORDER_TYPE: Alpaca has no trailing stops for options (use emulation_trigger)"
            if order.time_in_force not in (TimeInForce.DAY, TimeInForce.GTC):
                return "UNSUPPORTED_TIME_IN_FORCE: options orders must be DAY or GTC"
            if order.side == OrderSide.SELL and self._net_position(order.instrument_id) < order.quantity.as_decimal():
                return "SELL_TO_OPEN: selling options to open is not allowed"
        return None

    def _order_body(self, order: Order) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": order.instrument_id.symbol.value,
            "qty": str(int(order.quantity.as_decimal())),
            "side": self._enum_parser.parse_internal_order_side(order.side).value,
            "type": self._enum_parser.parse_internal_order_type(order.order_type).value,
            "time_in_force": self._enum_parser.parse_internal_time_in_force(order.time_in_force).value,
            "client_order_id": order.client_order_id.value,
        }
        if order.has_price:
            body["limit_price"] = str(order.price)
        if order.order_type == OrderType.TRAILING_STOP_MARKET:
            if order.trailing_offset_type == TrailingOffsetType.BASIS_POINTS:
                body["trail_percent"] = str(order.trailing_offset / 100)
            else:
                body["trail_price"] = str(order.trailing_offset)
        elif order.has_trigger_price:
            body["stop_price"] = str(order.trigger_price)
        if is_option_symbol(order.instrument_id.symbol.value):
            if order.side == OrderSide.BUY:
                intent = AlpacaPositionIntent.BUY_TO_CLOSE if self._net_position(order.instrument_id) < 0 else AlpacaPositionIntent.BUY_TO_OPEN
            else:
                intent = AlpacaPositionIntent.SELL_TO_CLOSE
            body["position_intent"] = intent.value
        elif "extended_hours=true" in (order.tags or []):
            body["extended_hours"] = True
        return body

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        if order.is_closed:
            self._log.warning(f"Cannot submit already closed order {order}")
            return
        reason = self._validate_order_pre_submit(order)
        if reason:
            self.generate_order_denied(
                order.strategy_id, order.instrument_id, order.client_order_id, reason, self._clock.timestamp_ns(),
            )
            return
        self.generate_order_submitted(order.strategy_id, order.instrument_id, order.client_order_id, self._clock.timestamp_ns())
        body = self._order_body(order)
        self._log.info(f"Submitting {body}", LogColor.MAGENTA)
        try:
            response = await self._http_account.orders.submit(body)
        except AlpacaError as e:
            if should_retry(e):
                # Unknown outcome: the engine's in-flight check resolves it by client order id
                self._log.warning(f"Submit of {order.client_order_id} uncertain ({e}); awaiting reconciliation")
            else:
                self.generate_order_rejected(
                    order.strategy_id, order.instrument_id, order.client_order_id, e.text, self._clock.timestamp_ns(),
                )
            return
        if response.status.value != "rejected":
            self._accept(order.strategy_id, order.instrument_id, order.client_order_id, VenueOrderId(response.id), self._clock.timestamp_ns())

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.order_list.orders:
            await self._submit_order(
                SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    order=order,
                    command_id=command.id,
                    ts_init=command.ts_init,
                    position_id=command.position_id,
                    client_id=command.client_id,
                ),
            )

    async def _modify_order(self, command: ModifyOrder) -> None:
        order = self._cache.order(command.client_order_id)
        venue_order_id = command.venue_order_id or (order.venue_order_id if order else None)
        if order is None or venue_order_id is None:
            self.generate_order_modify_rejected(
                command.strategy_id, command.instrument_id, command.client_order_id, venue_order_id,
                "ORDER_NOT_FOUND", self._clock.timestamp_ns(),
            )
            return
        body: dict[str, Any] = {}
        if command.quantity is not None:
            body["qty"] = str(int(command.quantity.as_decimal()))
        if command.price is not None:
            body["limit_price"] = str(command.price)
        if command.trigger_price is not None:
            body["trail" if order.order_type == OrderType.TRAILING_STOP_MARKET else "stop_price"] = str(command.trigger_price)
        try:
            replacement = await self._http_account.orders.replace(venue_order_id.value, body)
        except AlpacaError as e:
            self.generate_order_modify_rejected(
                command.strategy_id, command.instrument_id, command.client_order_id, venue_order_id,
                e.text, self._clock.timestamp_ns(),
            )
            return
        instrument = self._cache.instrument(order.instrument_id)
        precision = instrument.price_precision if instrument else 2
        self.generate_order_updated(
            command.strategy_id,
            command.instrument_id,
            command.client_order_id,
            VenueOrderId(replacement.id),
            Quantity.from_str(str(int(Decimal(replacement.qty or order.quantity.as_decimal())))),
            Price(float(replacement.limit_price), precision) if replacement.limit_price else None,
            Price(float(replacement.stop_price), precision) if replacement.stop_price else None,
            self._clock.timestamp_ns(),
            venue_order_id_modified=True,
        )

    async def _cancel_order(self, command: CancelOrder) -> None:
        order = self._cache.order(command.client_order_id)
        venue_order_id = command.venue_order_id or (order.venue_order_id if order else None)
        if venue_order_id is None:
            self.generate_order_cancel_rejected(
                command.strategy_id, command.instrument_id, command.client_order_id, None,
                "ORDER_NOT_ACKNOWLEDGED", self._clock.timestamp_ns(),
            )
            return
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            await retry_manager.run(
                "cancel_order",
                [command.client_order_id, venue_order_id],
                self._http_account.orders.cancel,
                venue_order_id.value,
            )
            if not retry_manager.result:
                self.generate_order_cancel_rejected(
                    command.strategy_id, command.instrument_id, command.client_order_id, venue_order_id,
                    retry_manager.message, self._clock.timestamp_ns(),
                )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        for order in self._cache.orders_open(venue=ALPACA_VENUE, instrument_id=command.instrument_id, side=command.order_side):
            await self._cancel_order(
                CancelOrder(
                    trader_id=command.trader_id,
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    command_id=command.id,
                    ts_init=command.ts_init,
                ),
            )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    # -- USER STREAM ------------------------------------------------------------------------------

    def _handle_user_ws_message(self, item: dict[str, Any]) -> None:
        if item.get("stream") != "trade_updates":
            return
        try:
            update = msgspec.convert(item, AlpacaTradeUpdate)
        except msgspec.ValidationError as e:
            self._log.error(f"Unparsable trade update {item}: {e}")
            return
        update.data.handle_trade_update(self)

    # -- RECONCILIATION REPORTS -------------------------------------------------------------------

    async def _order_report(self, order: AlpacaOrder) -> OrderStatusReport | None:
        if order.notional or (order.asset_class not in (None, "us_equity", "us_option")):
            self._log.warning(f"Skipping order {order.id} ({order.symbol}): notional or non-US-equity/option")
            return None
        instrument = await self._instrument_for(order.symbol)
        if instrument is None:
            return None
        return order.parse_to_order_status_report(
            account_id=self.account_id,
            instrument_id=instrument.id,
            price_precision=instrument.price_precision,
            client_order_id=self._client_order_id_for(order),
            ts_init=self._clock.timestamp_ns(),
            enum_parser=self._enum_parser,
        )

    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None:
        try:
            order = None
            if command.venue_order_id is not None:
                order = await self._http_account.orders.get(command.venue_order_id.value)
            elif command.client_order_id is not None:
                order = await self._http_account.orders.get_by_client_order_id(command.client_order_id.value)
            return await self._order_report(order) if order is not None else None
        except Exception as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

    async def _orders_since(self, start: pd.Timestamp | None, open_only: bool) -> list[AlpacaOrder]:
        orders = {o.id: o for o in await self._http_account.orders.list(status="open")}
        if not open_only:
            after = (start or pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
            for o in await self._http_account.orders.list(status="all", after=after):
                orders.setdefault(o.id, o)
        return [o for o in orders.values() if o.status.value != "replaced"]

    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []
        try:
            for order in await self._orders_since(command.start, command.open_only):
                if command.instrument_id and alpaca_symbol_to_instrument_id(order.symbol) != command.instrument_id:
                    continue
                report = await self._order_report(order)
                if report is not None:
                    reports.append(report)
        except Exception as e:
            self._log_report_error(e, "OrderStatusReports")
        self._log.info(f"Received {len(reports)} OrderStatusReport(s)")
        return reports

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        reports: list[FillReport] = []
        try:
            after = (command.start or pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
            orders = {o.id: o for o in await self._orders_since(command.start, open_only=False)}
            for fill in await self._http_account.query_fills(after=after):
                if command.venue_order_id and fill.order_id != command.venue_order_id.value:
                    continue
                if command.instrument_id and alpaca_symbol_to_instrument_id(fill.symbol) != command.instrument_id:
                    continue
                instrument = await self._instrument_for(fill.symbol)
                if instrument is None:
                    continue
                order = orders.get(fill.order_id)
                reports.append(fill.parse_to_fill_report(
                    account_id=self.account_id,
                    instrument_id=instrument.id,
                    price_precision=instrument.price_precision,
                    client_order_id=self._client_order_id_for(order) if order else None,
                    trade_id=TradeId(fill.id.split("::")[-1][:36]),
                    liquidity_side=LiquiditySide.MAKER if order and order.alpaca_type.value == "limit" else LiquiditySide.TAKER,
                    enum_parser=self._enum_parser,
                    ts_init=self._clock.timestamp_ns(),
                ))
        except Exception as e:
            self._log_report_error(e, "FillReports")
        self._log.info(f"Received {len(reports)} FillReport(s)")
        return reports

    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]:
        reports: list[PositionStatusReport] = []
        try:
            for position in await self._http_account.query_positions():
                if position.asset_class not in ("us_equity", "us_option"):
                    continue
                instrument_id = alpaca_symbol_to_instrument_id(position.symbol)
                if command.instrument_id and instrument_id != command.instrument_id:
                    continue
                if Decimal(position.qty) != Decimal(position.qty).to_integral_value():
                    self._log.warning(f"Skipping fractional position {position.symbol} {position.qty}")
                    continue
                instrument = await self._instrument_for(position.symbol)
                if instrument is not None:
                    reports.append(position.parse_to_position_status_report(self.account_id, instrument.id, self._clock.timestamp_ns()))
        except Exception as e:
            self._log_report_error(e, "PositionStatusReports")
        self._log.info(f"Received {len(reports)} PositionStatusReport(s)")
        return reports
