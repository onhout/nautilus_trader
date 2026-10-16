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

from enum import Enum
from enum import unique

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce


"""
Defines `Alpaca` specific enums.

References
----------
https://docs.alpaca.markets/reference/postorder
https://docs.alpaca.markets/docs/orders-at-alpaca

"""


@unique
class AlpacaEnvironment(Enum):
    """
    Represents an Alpaca environment (account).
    """

    PAPER = "PAPER"
    LIVE = "LIVE"


@unique
class AlpacaDataFeed(Enum):
    """
    Represents an Alpaca market data feed.

    ``IEX`` and ``INDICATIVE`` are free; ``SIP`` and ``OPRA`` need the Algo Trader Plus plan.
    """

    IEX = "iex"
    SIP = "sip"
    DELAYED_SIP = "delayed_sip"
    INDICATIVE = "indicative"
    OPRA = "opra"


@unique
class AlpacaAssetClass(Enum):
    US_EQUITY = "us_equity"
    US_OPTION = "us_option"
    CRYPTO = "crypto"


@unique
class AlpacaOrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@unique
class AlpacaPositionIntent(Enum):
    BUY_TO_OPEN = "buy_to_open"
    BUY_TO_CLOSE = "buy_to_close"
    SELL_TO_OPEN = "sell_to_open"
    SELL_TO_CLOSE = "sell_to_close"


@unique
class AlpacaOrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


@unique
class AlpacaTimeInForce(Enum):
    DAY = "day"
    GTC = "gtc"
    OPG = "opg"
    CLS = "cls"
    IOC = "ioc"
    FOK = "fok"


@unique
class AlpacaOrderStatus(Enum):
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    DONE_FOR_DAY = "done_for_day"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REPLACED = "replaced"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    ACCEPTED = "accepted"
    PENDING_NEW = "pending_new"
    ACCEPTED_FOR_BIDDING = "accepted_for_bidding"
    STOPPED = "stopped"
    REJECTED = "rejected"
    SUSPENDED = "suspended"
    CALCULATED = "calculated"
    HELD = "held"


@unique
class AlpacaTradeEvent(Enum):
    """
    Event types of the ``trade_updates`` stream.
    """

    NEW = "new"
    FILL = "fill"
    PARTIAL_FILL = "partial_fill"
    CANCELED = "canceled"
    EXPIRED = "expired"
    DONE_FOR_DAY = "done_for_day"
    REPLACED = "replaced"
    REJECTED = "rejected"
    PENDING_NEW = "pending_new"
    STOPPED = "stopped"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    CALCULATED = "calculated"
    SUSPENDED = "suspended"
    ORDER_REPLACE_REJECTED = "order_replace_rejected"
    ORDER_CANCEL_REJECTED = "order_cancel_rejected"
    ACCEPTED = "accepted"
    HELD = "held"


class AlpacaEnumParser:
    """
    Provides parsing methods for enums used by the 'Alpaca' adapter.
    """

    def __init__(self) -> None:
        self.ext_to_int_order_side = {
            AlpacaOrderSide.BUY: OrderSide.BUY,
            AlpacaOrderSide.SELL: OrderSide.SELL,
        }
        self.int_to_ext_order_side = {v: k for k, v in self.ext_to_int_order_side.items()}

        self.ext_to_int_order_type = {
            AlpacaOrderType.MARKET: OrderType.MARKET,
            AlpacaOrderType.LIMIT: OrderType.LIMIT,
            AlpacaOrderType.STOP: OrderType.STOP_MARKET,
            AlpacaOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
            AlpacaOrderType.TRAILING_STOP: OrderType.TRAILING_STOP_MARKET,
        }
        self.int_to_ext_order_type = {v: k for k, v in self.ext_to_int_order_type.items()}

        self.ext_to_int_time_in_force = {
            AlpacaTimeInForce.DAY: TimeInForce.DAY,
            AlpacaTimeInForce.GTC: TimeInForce.GTC,
            AlpacaTimeInForce.OPG: TimeInForce.AT_THE_OPEN,
            AlpacaTimeInForce.CLS: TimeInForce.AT_THE_CLOSE,
            AlpacaTimeInForce.IOC: TimeInForce.IOC,
            AlpacaTimeInForce.FOK: TimeInForce.FOK,
        }
        self.int_to_ext_time_in_force = {v: k for k, v in self.ext_to_int_time_in_force.items()}

        self.ext_to_int_order_status = {
            AlpacaOrderStatus.NEW: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.ACCEPTED: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.PENDING_NEW: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.ACCEPTED_FOR_BIDDING: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.HELD: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.CALCULATED: OrderStatus.ACCEPTED,
            AlpacaOrderStatus.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
            AlpacaOrderStatus.FILLED: OrderStatus.FILLED,
            AlpacaOrderStatus.DONE_FOR_DAY: OrderStatus.CANCELED,
            AlpacaOrderStatus.CANCELED: OrderStatus.CANCELED,
            AlpacaOrderStatus.STOPPED: OrderStatus.CANCELED,
            AlpacaOrderStatus.SUSPENDED: OrderStatus.CANCELED,
            AlpacaOrderStatus.REPLACED: OrderStatus.CANCELED,
            AlpacaOrderStatus.EXPIRED: OrderStatus.EXPIRED,
            AlpacaOrderStatus.REJECTED: OrderStatus.REJECTED,
            AlpacaOrderStatus.PENDING_CANCEL: OrderStatus.PENDING_CANCEL,
            AlpacaOrderStatus.PENDING_REPLACE: OrderStatus.PENDING_UPDATE,
        }

    def parse_alpaca_order_side(self, order_side: AlpacaOrderSide) -> OrderSide:
        return self.ext_to_int_order_side[order_side]

    def parse_internal_order_side(self, order_side: OrderSide) -> AlpacaOrderSide:
        return self.int_to_ext_order_side[order_side]

    def parse_alpaca_order_type(self, order_type: AlpacaOrderType) -> OrderType:
        return self.ext_to_int_order_type[order_type]

    def parse_internal_order_type(self, order_type: OrderType) -> AlpacaOrderType | None:
        return self.int_to_ext_order_type.get(order_type)

    def parse_alpaca_time_in_force(self, time_in_force: AlpacaTimeInForce) -> TimeInForce:
        return self.ext_to_int_time_in_force[time_in_force]

    def parse_internal_time_in_force(self, time_in_force: TimeInForce) -> AlpacaTimeInForce | None:
        return self.int_to_ext_time_in_force.get(time_in_force)

    def parse_alpaca_order_status(self, order_status: AlpacaOrderStatus) -> OrderStatus:
        return self.ext_to_int_order_status[order_status]
