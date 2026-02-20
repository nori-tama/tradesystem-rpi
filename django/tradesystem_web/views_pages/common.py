import sys
from pathlib import Path

COMMON_DIR = Path(__file__).resolve().parents[3] / 'scripts' / 'common'
if str(COMMON_DIR) not in sys.path:
    sys.path.append(str(COMMON_DIR))

from exchange_calendar import calculate_exchange_business_days, shift_exchange_business_day


def format_market_label(market):
    if not market:
        return '-'

    return (
        str(market)
        .replace('プライム', 'Ｐ')
        .replace('スタンダード', 'Ｓ')
        .replace('グロース', 'Ｇ')
        .replace('（内国株式）', '（内）')
        .replace('（外国株式）', '（外）')
    )
