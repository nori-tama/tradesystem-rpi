import sys
from pathlib import Path

COMMON_DIR = Path(__file__).resolve().parents[3] / 'scripts' / 'common'
if str(COMMON_DIR) not in sys.path:
    sys.path.append(str(COMMON_DIR))

from exchange_calendar import calculate_exchange_business_days, shift_exchange_business_day
