# EventCalendar

A simple Python library for fetching earnings, dividends, IPOs, economic events, and splits data from the NASDAQ API.

## Installation

```sh
pip install event-calendar
```

### Usage

```py
from event_calendar import EventCalendar

ec = EventCalendar()

# Get earnings for a specific date
earnings = ec.get_earnings("2024-04-17")
print(earnings)

# Get earnings for a range of dates
for df in ec.get_earnings_range("2024-04-17", "2024-04-20"):
    print(df)
    df.to_csv("earnings.csv", mode="a", index=False) # save to csv

# Get dividends for a specific date
dividends = ec.get_dividends("2023-10-03")
print(dividends)

# Get dividends for a date range
for df in ec.get_dividends_range("2024-04-17", "2024-04-20"):
    print(df)

# Get IPO data for a specific month
ipo = ec.get_ipo("2025-04")
print(ipo)

# Get IPOs for a range of months
for df in ec.get_ipo_range("2024-04", "2024-06"):
    print(df)

# Get economic events for a specific date
economic_events = ec.get_economic_calendar("2025-01-01")
print(economic_events)

# Get economic events for a range of dates
for df in ec.get_economic_calendar_range("2024-04-17", "2024-04-20"):
    print(df)

# Get current stock splits
splits = ec.get_splits()
print(splits)
```
