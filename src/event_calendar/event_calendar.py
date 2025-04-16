from typing import Dict, Union, Optional, Generator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime
import time


class EventCalendarError(Exception):
    """Custom exception for EventCalendar errors."""

    pass


class EventCalendar:
    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
        retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.base_url = "https://api.nasdaq.com/api"
        self.earnings_url = f"{self.base_url}/calendar/earnings"
        self.dividends_url = f"{self.base_url}/calendar/dividends"
        self.ipo_url = f"{self.base_url}/ipo/calendar"
        self.economic_calendar = f"{self.base_url}/calendar/economicevents"
        self.splits_url = f"{self.base_url}/calendar/splits"

        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        }

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        retry = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _fetch(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Dict:
        """Internal method to fetch data from the given URL.

        Args:
            url (str): URL to fetch data from.
            params (Optional[Dict[str, Union[str, int]]], optional): Query parameters to include in the request. Defaults to None.

        Raises:
            EventCalendarError: Error fetching data from the URL.
            EventCalendarError: Error decoding JSON response.

        Returns:
            Dict: Response data in JSON format.
        """
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise EventCalendarError(f"Error fetching data from {url}: {str(e)}")
        except ValueError:
            raise EventCalendarError(f"Error decoding JSON response from {url}")

    def _status_verify(self, response: Dict) -> Dict:
        """Internal method to verify the status of the response.


        Args:
            response (Dict): Response data.
        Raises:
            EventCalendarError: Error in the response status.

        Returns:
            Dict: Response data.
        """
        status = response.get("status")
        bCodeMessage = status.get("bCodeMessage")
        if bCodeMessage is not None:
            raise EventCalendarError(bCodeMessage[0]["errorMessage"])

        return response

    def get_earnings(
        self,
        date: str,
    ) -> pd.DataFrame:
        """Get earnings data for a specific date.

        Example:
            >>> date = "2025-04-11"
        Args:
            date (str): Accepts date in YYYY-MM-DD format.

        Raises:
            EventCalendarError: Error fetching earnings data.

        Returns:
            pd.DataFrame: DataFrame containing earnings data.
        """
        params = {"date": date}
        response = self._fetch(self.earnings_url, params=params)
        response = self._status_verify(response)
        earnings_date = response.get("data", {})["asOf"]
        earnings_data = response.get("data", {}).get("rows", [])

        df = pd.DataFrame(earnings_data)
        if df.empty:
            raise EventCalendarError("No earnings data found.")
        df.insert(0, "date", pd.to_datetime(earnings_date))

        return df

    def get_earnings_range(
        self,
        start_date: str,
        end_date: str,
        sleep: Union[float | int] = 0.1,
    ) -> Generator[pd.DataFrame, None, None]:
        """Get earnings data for a range of dates.

        Example:
            >>> start_date = "2025-04-11"
            >>> end_date = "2025-04-17"

        Args:
            start_date (str): Accept date in YYYY-MM-DD format.
            end_date (str): Accept date in YYYY-MM-DD format.
            sleep (float | int): Sleep time between requests. Defaults to 0.1.

        Raises:
            EventCalendarError: Error fetching earnings data.

        Yields:
            Generator[pd.DataFrame, None, None]: DataFrame containing earnings data for each date in the range.
        """

        business_date_range = pd.bdate_range(start_date, end_date)

        for date in business_date_range:
            date = date.strftime("%Y-%m-%d")
            try:
                earnings_data = self.get_earnings(date)
                yield earnings_data
            except EventCalendarError as e:
                print(f"Error fetching earnings data for {date}: {e}")
            time.sleep(sleep)

    def get_dividends(self, date: str) -> pd.DataFrame:
        """Get dividends data for a specific date.

        Example:
            >>> date = "2025-04-11"
        Args:
            date (str): Accept date in YYYY-MM-DD format.

        Raises:
            EventCalendarError: Error fetching dividends data.

        Returns:
            pd.DataFrame: DataFrame containing dividends data.
        """
        params = {"date": date}
        response = self._fetch(self.dividends_url, params=params)
        response = self._status_verify(response)
        dividends_date = response.get("data", {}).get("calendar", {})["asOf"]
        dividends_data = response.get("data", {}).get("calendar", {}).get("rows", [])

        df = pd.DataFrame(dividends_data)
        if df.empty:
            raise EventCalendarError("No dividends data found.")
        df.insert(0, "date", pd.to_datetime(dividends_date))

        return df

    def get_dividends_range(
        self,
        start_date: str,
        end_date: str,
        sleep: Union[float | int] = 0.1,
    ) -> Generator[pd.DataFrame, None, None]:
        """Get dividends data for a range of dates.

        Example:
            >>> start_date = "2025-04-11"
            >>> end_date = "2025-04-17"

        Args:
            start_date (str): Accept date in YYYY-MM-DD format.
            end_date (str): Accept date in YYYY-MM-DD format.
            sleep (float | int): Sleep time between requests. Defaults to 0.1.

        Raises:
            EventCalendarError: Error fetching dividends data.

        Yields:
            Generator[pd.DataFrame, None, None]: DataFrame containing dividends data for each date in the range.
        """

        business_date_range = pd.bdate_range(start_date, end_date)

        for date in business_date_range:
            date = date.strftime("%Y-%m-%d")
            try:
                dividends_data = self.get_dividends(date)
                yield dividends_data
            except EventCalendarError as e:
                print(f"Error fetching dividends data for {date}: {e}")
            time.sleep(sleep)

    def get_ipo(self, date: str) -> pd.DataFrame:
        """Get IPO data for a specific date.

        Example:
            >>> date = "2025-04"
        Args:
            date (str): Accept date in YYYY-MM format.

        Raises:
            EventCalendarError: Error fetching IPO data.

        Returns:
            pd.DataFrame: DataFrame containing IPO data.
        """

        params = {"date": date}
        response = self._fetch(self.ipo_url, params=params)
        response = self._status_verify(response)
        # ipo_date = response.get("data", {}).get("priced", {}).get("asOf") or date
        ipo_data = response.get("data", {}).get("priced", {}).get("rows", [])

        df = pd.DataFrame(ipo_data)
        if df.empty:
            raise EventCalendarError("No IPO data found.")
        # year_month = ipo_date[:7]
        # df.insert(0, "date", year_month)

        return df

    def get_ipo_range(
        self,
        start_date: str,
        end_date: str,
        sleep: Union[float | int] = 0.1,
    ) -> Generator[pd.DataFrame, None, None]:
        """Get IPO data for a range of dates.

        Example:
            >>> start_date = "2025-04"
            >>> end_date = "2025-06"

        Args:
            start_date (str): Accept date in YYYY-MM format.
            end_date (str): Accept date in YYYY-MM format.
            sleep (float | int, optional): Sleep time between requests. Defaults to 0.1.

        Raises:
            EventCalendarError: Error fetching IPO data.

        Yields:
            Generator[pd.DataFrame, None, None]: DataFrame containing IPO data for each month in the range.
        """
        start_date = datetime.strptime(start_date, "%Y-%m")
        end_date = datetime.strptime(end_date, "%Y-%m")

        month_range = pd.date_range(start=start_date, end=end_date, freq="MS")

        for date in month_range:
            date = date.strftime("%Y-%m")
            try:
                ipo_data = self.get_ipo(date)
                yield ipo_data
            except EventCalendarError as e:
                print(f"Error fetching IPO data for {date}: {e}")
            time.sleep(sleep)

    def get_economic_calendar(self, date: str) -> pd.DataFrame:
        """Get economic calendar data for a specific date.
        Example:
            >>> date = "2025-01-01"

        Args:
            date (str): Accept date in YYYY-MM-DD format.

        Raises:
            EventCalendarError: Error fetching economic calendar data.

        Returns:
            pd.DataFrame: DataFrame containing economic calendar data.
        """
        params = {"date": date}
        response = self._fetch(self.economic_calendar, params=params)
        response = self._status_verify(response)
        # economic_calendat_date = response.get("data", {})["asOf"]
        economic_calendat_date = date
        economic_calendat_data = response.get("data", {}).get("rows", [])
        df = pd.DataFrame(economic_calendat_data)
        if df.empty:
            raise EventCalendarError("No economic events data found.")
        df.insert(0, "date", pd.to_datetime(economic_calendat_date))

        return df

    def get_economic_calendar_range(
        self,
        start_date: str,
        end_date: str,
        sleep: Union[float | int] = 0.1,
    ) -> Generator[pd.DataFrame, None, None]:
        """Get economic calendar data for a range of dates.

        Example:
            >>> start_date = "2025-01-01"
            >>> end_date = "2025-01-31"

        Args:
            start_date (str): Accept date in YYYY-MM-DD format.
            end_date (str): Accept date in YYYY-MM-DD format.

            sleep (float | int, optional): Sleep time between requests. Defaults to 0.1.

        Raises:
            EventCalendarError: Error fetching economic calendar data.

        Yields:
            Generator[pd.DataFrame, None, None]: _description_
        """
        business_date_range = pd.bdate_range(start_date, end_date)

        for date in business_date_range:
            date = date.strftime("%Y-%m-%d")
            try:
                economic_calendat_data = self.get_economic_calendar(date)
                yield economic_calendat_data
            except EventCalendarError as e:
                print(f"Error fetching economic calendar data for {date}: {e}")
            time.sleep(sleep)

    def get_splits(self) -> pd.DataFrame:
        """Get splits data.

        Raises:
            EventCalendarError: Error fetching splits data.

        Returns:
            pd.DataFrame: DataFrame containing splits data.
        """
        response = self._fetch(self.splits_url)
        response = self._status_verify(response)
        splits_data = response.get("data", {}).get("rows", [])
        df = pd.DataFrame(splits_data)
        if df.empty:
            raise EventCalendarError("No splits data found.")

        return df


# if __name__ == "__main__":

# ec = EventCalendar()

# # Get earnings for a specific date
# earnings = ec.get_earnings("2024-04-17")
# print(earnings)

# # Get earnings for a range of dates
# for df in ec.get_earnings_range("2024-04-17", "2024-04-20"):
#     print(df)
#     df.to_csv("earnings.csv", mode="a", index=False)

# # Get dividends for a specific date
# dividends = ec.get_dividends("2023-10-03")
# print(dividends)

# # Get dividends for a date range
# for df in ec.get_dividends_range("2024-04-17", "2024-04-20"):
#     print(df)

# # Get IPO data for a specific month
# ipo = ec.get_ipo("2025-04")
# print(ipo)

# # Get IPOs for a range of months
# for df in ec.get_ipo_range("2024-04", "2024-06"):
#     print(df)

# # Get economic events for a specific date
# economic_events = ec.get_economic_calendar("2025-01-01")
# print(economic_events)

# # Get economic events for a range of dates
# for df in ec.get_economic_calendar_range("2024-04-17", "2024-04-20"):
#     print(df)

# # Get current stock splits
# splits = ec.get_splits()
# print(splits)
