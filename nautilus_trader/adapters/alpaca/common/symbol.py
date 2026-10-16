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

import datetime as dt
import re
from decimal import ROUND_DOWN
from decimal import ROUND_HALF_UP
from decimal import ROUND_UP
from decimal import Decimal

from nautilus_trader.adapters.alpaca.common.constants import ALPACA_PENNY_EVERYWHERE
from nautilus_trader.adapters.alpaca.common.constants import ALPACA_VENUE
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol


# OCC option symbol without padding: root, YYMMDD, C/P, strike x 1000 in 8 digits
# (e.g. MU261030C00250000 is the MU 2026-10-30 250 call), as Alpaca uses it.
_OCC_SYMBOL = re.compile(r"^([A-Z][A-Z.]{0,5})(\d{6})([CP])(\d{8})$")


def alpaca_symbol_to_instrument_id(symbol: str) -> InstrumentId:
    return InstrumentId(Symbol(symbol.upper()), ALPACA_VENUE)


def occ_symbol(root: str, expiration: str, kind: str, strike: Decimal | str | float) -> str:
    """
    Return the OCC symbol: ``occ_symbol('MU', '2026-10-30', 'call', '250')`` -> ``MU261030C00250000``.
    """
    exp = dt.date.fromisoformat(expiration)
    strike_milli = int((Decimal(str(strike)) * 1000).to_integral_value())
    return f"{root.upper()}{exp:%y%m%d}{'C' if kind.lower().startswith('c') else 'P'}{strike_milli:08d}"


def parse_occ_symbol(symbol: str) -> tuple[str, str, str, Decimal] | None:
    """
    Parse an OCC symbol into (root, expiration ISO date, 'call'|'put', strike), or None for equities.
    """
    m = _OCC_SYMBOL.match(symbol)
    if m is None:
        return None
    root, yymmdd, cp, strike = m.groups()
    expiration = dt.datetime.strptime(yymmdd, "%y%m%d").date().isoformat()  # noqa: DTZ007
    value = Decimal(strike) / 1000
    value = value.quantize(Decimal(1)) if value == value.to_integral_value() else value.normalize()
    return root, expiration, "call" if cp == "C" else "put", value


def is_option_symbol(symbol: str) -> bool:
    return _OCC_SYMBOL.match(symbol) is not None


def option_tick_table(root: str, penny_pilot: bool | None) -> dict[str, str]:
    """
    Price increments Alpaca routes without rejection for an option class.

    SPY/QQQ/IWM trade in pennies at any price; penny-pilot classes (``ppind``) in $0.01 below
    $3 and $0.05 above; all other classes in $0.05 below $3 and $0.10 above.
    """
    if root.upper() in ALPACA_PENNY_EVERYWHERE:
        return {"below_tick": "0.01", "above_tick": "0.01", "cutoff_price": "3.00"}
    if penny_pilot:
        return {"below_tick": "0.01", "above_tick": "0.05", "cutoff_price": "3.00"}
    return {"below_tick": "0.05", "above_tick": "0.10", "cutoff_price": "3.00"}


def option_tick(price: Decimal, tick_table: dict | None) -> Decimal:
    table = tick_table or {}
    below = Decimal(str(table.get("below_tick") or "0.05"))
    above, cutoff = table.get("above_tick"), table.get("cutoff_price")
    if above and cutoff and price >= Decimal(str(cutoff)):
        return Decimal(str(above))
    return below


def round_option_price(price: Decimal | float, tick_table: dict | None, side: str = "nearest") -> Decimal:
    """
    Round a price to a valid option tick ('up' for buys that must not undershoot, 'down' for sells).
    """
    price = Decimal(str(price))
    tick = option_tick(price, tick_table)
    rounding = {"up": ROUND_UP, "down": ROUND_DOWN}.get(side, ROUND_HALF_UP)
    steps = (price / tick).to_integral_value(rounding=rounding)
    return max(tick, steps * tick).quantize(Decimal("0.01"))
