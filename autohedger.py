"""
autohedger.py:
    A Basic Auto Hedger process for Spot Bitcoin and Ethereum.
    See README.md file for more details.

Author: Matt Webb
Version: 1.0
"""

import pandas as pd
from datetime import datetime
from datetime import timedelta
from typing import Final, List

from pandas_datareader import data
from collections import Counter

TRADE_DIRECTION_BUY: Final[str] = "buy"
TRADE_DIRECTION_SELL: Final[str] = "sell"

SUPPORTED_ASSETS: Final[List[str]] = ["BTC", "ETH"]
SUPPORTED_FIAT: Final[List[str]] = ["USD"]

STRATEGY_SLOW: Final[str] = "SLOW"
STRATEGY_NORMAL: Final[str] = "NORMAL"
STRATEGY_STEALTH: Final[str] = "STEALTH"
STRATEGY_PARAMETERS: Final[dict] = {
    STRATEGY_SLOW: {
        "client_volume_max": 1e5,  # 100k USD notional
        "time_window": 10  # 10-minute trading window
    },
    STRATEGY_STEALTH: {
        "client_volume_trigger": 1e7,  # 10m USD notional
        "execution_chunks": 5  # Hedge in 5 chunks
    }
}
SUPPORTED_STRATEGIES: Final[List[str]] = [STRATEGY_SLOW, STRATEGY_NORMAL, STRATEGY_STEALTH]

PRICES_SOURCE = "yahoo"


class Trade:
    """
    Represents a trade.
    ...
    Attributes
    ----------
    book: str
        The book in which the trade is booked.
    direction: str
        The direction of the trade, e.g. Buy/Sell.
    trade_time: datetime
        The execution time of the trade in datetime format, e.g. 2022/05/30 17:00:.
    asset: str
        The traded asset of the trade, e.g. BTC, ETH.
    denominated: str
        The FIAT denomination of the trade, e.g. USD, EUR. Initial implementation only supports USD.
    quantity: float
        The quantity of the trade.
    price: float
        The price the trade was executed at.
    Methods
    -------
    get_position():
        Returns a dictionary with the position of the trade.
    """
    def __init__(self,
                 book: str, trade_time: datetime, asset: SUPPORTED_ASSETS, denominated: SUPPORTED_FIAT,
                 quantity: float, price: float):
        """
        Constructor for Trade
        :param book: The book the Trade is booked into
        :param trade_time: Trade execution/booking time
        :param asset: The traded security
        :param denominated: Unit of the price
        :param quantity: Quantity of the traded asset
        :param price: Price of execution
        """
        self.book = book
        self.direction = TRADE_DIRECTION_BUY if quantity >= 0 else TRADE_DIRECTION_SELL
        self.trade_time = trade_time
        self.asset = asset
        self.denominated = denominated
        self.quantity = quantity
        self.price = price

    def get_position(self) -> dict:
        """
        Gets the net position of the Trade.
        :return: Dictionary of net position.
        """
        return {self.asset: self.quantity}


class Book:
    """
    Represents a trading book.
    ...
    Attributes
    ----------
    name: str
        The short name of the book.
    positions: dict
        A dictionary of Position objects keyed by asset name.
    Methods
    -------
    trade_add(trade):
        Adds a given trade to the relevant position.
    get_positions(as_of_time):
        Returns dictionary of positions, as of a time if passed.
    """
    def __init__(self, name: str):
        """
        Constructor for Book class
        :param name: Assigned name for the book
        """
        self.name = name
        self.positions = {}

    def __str__(self) -> str:
        """
        String representation of this object
        :return: String with current position details
        """
        description = ""
        for position in self.positions.values():
            description += str(position)
        return description

    def trade_add(self, trade: Trade) -> None:
        """
        Books a given trade into this book
        :param trade: Trade object for booking
        :return: returns True if trade add is successful, False otherwise
        """
        # Create a new position if one does not exist already,
        # otherwise add to the existing position.
        asset = trade.asset
        if asset not in self.positions:
            self.positions[asset] = Position(asset)
        self.positions[asset].trade_add(trade)

    def get_positions(self, as_of_time=None) -> dict:
        """
        Returns the positions of the book as of a given time (default datetime.now()).
        :param as_of_time: Positions at this time, default None
        :return: Dictionary of position info.
        """
        positions = {}
        for asset, position in self.positions.items():
            positions[asset] = position.get_quantity(as_of_time)
        return positions

    def get_trades(self, as_of_time=None) -> pd.DataFrame:
        """
        Returns trades from all positions.
        :return: DataFrame of trades from all positions
        """
        if as_of_time is None:
            as_of_time = datetime.now()

        trades = pd.DataFrame()
        for asset, position in self.positions.items():
            trades = pd.concat([trades, position.get_trades(as_of_time)])
        trades = trades.sort_values(by="trade_time")
        return trades


class Portfolio:
    """
    Represents a collection of Book objects.
    ...
    Attributes
    ----------
    name: str
        The name of the portfolio.
    books: List[Book]
        The list of Book objects within this Portfolio.
    Methods
    -------
    net_positions: dict
        Returns the total net positions of all the books.
    """
    def __init__(self, name: str, books: List[Book]):
        """
        Constructor for Portfolio class
        :param name: Descriptive name for the Portfolio
        :param books: List of Books in this Portfolio
        """
        self.name = name
        self.books = books

    def net_positions(self, as_of_time=None) -> dict:
        """
        Get the net positions from all books
        :param as_of_time: Time to filter the list of trades, default is datetime.now()
        :return: Dictionary of positions from all books
        """
        counter = Counter()
        for book in self.books:
            book_positions = book.get_positions(as_of_time)
            counter.update(book_positions)
        return counter


class Position:
    """
    Represents a position.
    ...
    Attributes
    ----------
    asset: SUPPORTED_ASSETS
        The underlying asset of the position, e.g. BTC.
    quantity: float
        Current total of all trades executed.
    trades: List[Trade]
        List of trades contributing to the position.
    Methods
    -------
    trade_add(trade):
        Adds a given trade to the position, given an object of type Trade.
    get_trades_as_dataframe:
        Returns a data frame of all the trades in the position.
    get_trades(as_of_time=datetime.now()):
        Returns a data frame of the trades in the position as of a given time (defaults to datetime.now()).
    get_quantity():
        Returns the quantity of the position, using trade list with an as of filter if passed.
    """
    def __init__(self, asset: SUPPORTED_ASSETS):
        """
        Constructor for Position class
        :param asset: The tradable asset of this Position
        """
        self.asset = asset
        self.quantity = 0
        self.trades = []

    def __str__(self) -> str:
        """
        String representation of this object.
        :return: String with current quantity details.
        """
        return self.asset + ": " + str(self.quantity)

    def trade_add(self, trade: Trade) -> None:
        """
        Adds a given trade to this position and updates all relevant attributes
        :param trade: The trade to add to this position
        """
        self.trades.append(trade)
        self.quantity += trade.quantity

    def get_trades_as_dataframe(self) -> pd.DataFrame:
        """
        Gets the trades list as a DataFrame
        :return: DataFrame of trades from trades list
        """
        trades = [{"trade_time": trade.trade_time, "book": trade.book, "direction": trade.direction,
                   "asset": trade.asset, "denominated": trade.denominated, "quantity": trade.quantity,
                   "price": trade.price}
                  for trade in self.trades]
        return pd.DataFrame(trades)

    def get_trades(self, as_of_time=datetime.now()) -> pd.DataFrame:
        """
        Return a DataFrame of trades in the position, filtered by as_of_time
        which defaults to datetime.now()
        :param as_of_time: Time to filter the list of trades, default is datetime.now()
        :return: DataFrame of trades as of the as_of_time param
        """
        trades = self.get_trades_as_dataframe()
        trades_filtered = trades[trades["trade_time"] <= as_of_time]
        return trades_filtered

    def get_quantity(self, as_of_time=None) -> float:
        """
        Get the position quantity, using as_of_time if passed.
        If not passed, trivially return quantity attribute
        :param as_of_time: Filter on trades before this time
        :return: The total quantity of the position at as_of_time
        """
        if as_of_time is None:
            return self.quantity
        else:
            trades = self.get_trades(as_of_time)
            return trades["quantity"].sum()


class Exchange:
    """
    A class to represent a trading venue.
    ...
    Attributes
    ----------
    name : str
        The name of the exchange.
    supported_assets : list
        The list of markets that this exchange supports, also see SUPPORTED_ASSETS.
    Methods
    -------
    get_close_prices():
        Load closing prices for assets from Yahoo Finance.
    at_market_order(book, asset, quantity):
        Allows an order to be created in the market. Returns trade to book.
    """
    def __init__(self, name: str):
        """
        Constructor for Exchange class
        :param name: The name of the exchange
        """
        self.name = name
        self.supported_assets = SUPPORTED_ASSETS
        self.close_prices = self.get_close_prices()

    @staticmethod
    def get_close_prices() -> dict:
        """
        Get closing prices from Yahoo Finance for supported assets
        :return: Dictionary of closing prices
        """
        start_date = datetime.today() + timedelta(days=-1)
        end_date = datetime.today()
        closing_prices = {}
        for asset in SUPPORTED_ASSETS:
            close_data = data.DataReader(asset + "-USD", PRICES_SOURCE, start_date, end_date)
            last_close = close_data["Close"].head(1)
            closing_prices[asset] = last_close.values[0]
        return closing_prices

    def at_market_order(self, book: str, asset: str, quantity: float, trade_time: datetime) -> Trade:
        """
        Submit an order at market and return the Trade
        :param book: The book to book the resulting Trade into
        :param asset: The asset of the order
        :param quantity: The quantity of the order
        :param trade_time: Trade execution time
        :return: Returns a Trade if filled
        """
        price = self.close_prices[asset]
        trade = Trade(book, trade_time, asset, "USD", quantity, price)
        return trade


class Hedger:
    """
    A class to represent an auto hedging process.
    ...
    Attributes
    ----------
    exchange: Exchange
        Connected Exchange for execution.
    client_trades_file: str
        File path for client trades file.
    client_trades: DataFrame
        The client trades loaded from client_trades_file into a DataFrame.
    client_book: Book
        The Book for holding client positions.
    hedge_book: Book
        The Book for holding hedge positions.
    portfolio: Portfolio
        The Portfolio of books to hedge.
    start_time: datetime
        When to start the hedger.
    end_time: datetime
        When to stop the hedger.
    execution_queue: List
        A queue of hedging trades.
    Methods
    -------
    log(message):
        Simple standard logging method.
    trades_net_by_position(trades):
        Returns the net positions from a DataFrame of trades.
    trades_net_by_notional(trades):
        Returns the net notionals from a DataFrame of trades.
    load_client_trades(file):
        Loads a given csv file of client trades for hedging.
    client_trades_by_time(start_time, end_time):
        Returns a DataFrame of client trades within the given time window.
    book_client_trades(start_time, end_time):
        Books a set of trades within a time window and returns the list of trades as a DataFrame.
    recent_client_volume(sampling_window, as_of_time):
        Returns the client volume within the given window.
    get_current_strategy(hedger_time, client_notional_change):
        Returns the current hedging strategy.
    get_hedge_quantities_normal(current_positions):
        Returns the hedge quantities based on current positions for NORMAL strategy.
    get_hedge_quantities_stealth(current_positions):
        Returns the hedge quantities based on current positions for STEALTH strategy.
    get_hedge_quantities(current_positions, client_position_change, strategy):
        Main method for getting quantities for hedges, based on strategy.
    place_hedge_orders(hedge_positions):
        Interface to the Exchange for order placement.
    run_hedging():
        Runs the main hedging logic.
    save_hedge_trades():
        Saves the executed hedge trades to a csv.
    run():
        Main method for the class.
    """
    def __init__(self, client_trades_file: str):
        """
        Constructor for Hedger class
        :param client_trades_file: The path to the client trades file
        """
        self.exchange = Exchange("MY EXCHANGE")
        self.client_trades_file = client_trades_file
        self.client_trades = pd.DataFrame()
        self.client_book = Book("CLIENT BOOK")
        self.hedge_book = Book("HEDGE BOOK")
        self.portfolio = Portfolio("Hedging Portfolio", [self.client_book, self.hedge_book])
        self.start_time = None
        self.end_time = None
        self.execution_queue = []

    @staticmethod
    def log(message: str) -> None:
        """
        Standardized logging function
        :return: None
        """
        print(str(datetime.now()) + ": " + message)

    @staticmethod
    def trades_net_by_position(trades: pd.DataFrame) -> dict:
        """
        Net position of given trades DataFrame
        :param trades: DataFrame of trades
        :return: Dictionary of net positions
        """
        net_position = Counter()
        for index, row in trades.iterrows():
            net_position.update({row["asset"]: row["quantity"]})
        return net_position

    @staticmethod
    def trades_net_by_notional(trades: pd.DataFrame) -> dict:
        """
        Net notional of given trades DataFrame
        :param trades: DataFrame of trades
        :return: Dictionary of net notionals
        """
        net_notional = Counter()
        for index, row in trades.iterrows():
            net_notional.update({row["asset"]: row["quantity"]*row["price"]})
        return net_notional

    @staticmethod
    def get_hedge_quantities_normal(current_positions: dict):
        """
        Gets basic hedge quantities based on current positions
        :param current_positions: Dictionary of current positions
        :return: Dictionary of quantities to hedge
        """
        hedge_quantities = {}
        for asset, current_position in current_positions.items():
            if current_position != 0:
                hedge_quantities[asset] = (-1) * current_position
        return hedge_quantities

    def log_hedging_parameters(self) -> None:
        """
        Simple method to print the hedging parameters,
        used on start up.
        """
        self.log("Hedger Parameters:")
        self.log("STRATEGY SLOW:")
        self.log("Client Flow Max: " + str(STRATEGY_PARAMETERS[STRATEGY_SLOW]["client_volume_max"]))
        self.log("Client Flow Time Window: " + str(STRATEGY_PARAMETERS[STRATEGY_SLOW]["time_window"]))
        self.log("STRATEGY STEALTH:")
        self.log("Client Volume Trigger: " + str(STRATEGY_PARAMETERS[STRATEGY_STEALTH]["client_volume_trigger"]))
        self.log("Execution Chunks: " + str(STRATEGY_PARAMETERS[STRATEGY_STEALTH]["execution_chunks"]))

    def load_client_trades(self) -> None:
        """
        Loads trades from the client trades file and books
        them in the client book.
        """
        # Date formatter
        def date_parser(x): return datetime.strptime(x, "%Y/%m/%d %H:%M:%S")

        # Throws a FileNotFoundError if the file does not exist
        self.client_trades = pd.read_csv(self.client_trades_file, parse_dates=["trade_time"], date_parser=date_parser)
        self.client_trades["notional"] = self.client_trades["quantity"] * self.client_trades["price"]
        self.client_trades = self.client_trades.sort_values(by="trade_time")

        # Initialise the hedger time to just before first client trade
        self.start_time = self.client_trades["trade_time"].min() + timedelta(minutes=-1)
        self.end_time = self.client_trades["trade_time"].max()

    def client_trades_by_time(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """
        Provides a client of client trades filtered by time range given
        :param start_time: Start time of the trades window
        :param end_time: End time of the trades window
        :return: DataFrame of client trades filtered by the time window given
        """
        mask = (self.client_trades["trade_time"] > start_time) & (self.client_trades["trade_time"] <= end_time)
        client_trades_filtered = self.client_trades[mask]
        return client_trades_filtered

    def book_client_trades(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """
        Book the client trades between given start_time and end_time
        :param start_time: Start time of the window
        :param end_time: End time of the window
        :return: DataFrame of the trades booked
        """
        trades_to_book = self.client_trades_by_time(start_time, end_time)
        for index, row in trades_to_book.iterrows():
            trade = Trade(self.client_book.name, row["trade_time"], row["asset"],
                          row["denominated"], row["quantity"], row["price"])
            self.client_book.trade_add(trade)
        return trades_to_book

    def recent_client_volume(self, sampling_window: int, as_of_time=None) -> float:
        """
        Get the recent total client volume for a given window (in minutes) and
        at as of time
        :param as_of_time: Time to sample the client, defaults to datetime.now()
        :param sampling_window: Number of minutes to sample client activity
        :return: Total notional client volume for the given window at the given time
        """
        # Only accept a positive sampling window
        if sampling_window < 0:
            raise Exception("sampling_window must be positive number of minutes")

        # Default as_of_time to now if not passed
        if as_of_time is None:
            as_of_time = datetime.now()

        # Time window begins at passed as of time for the
        # given number of minutes back from that
        end_time = as_of_time
        start_time = end_time + timedelta(minutes=-sampling_window)

        # Return volume in notional terms (quantity * price)
        filtered_client_trades = self.client_trades_by_time(start_time, end_time)
        total_volume = filtered_client_trades["notional"].sum()

        return total_volume

    def get_current_strategy(self, hedger_time: datetime, client_notional_change: dict) -> SUPPORTED_STRATEGIES:
        """
        Gets the current strategy for the hedger based on
        given client book positions
        :param hedger_time: The hedger time
        :param client_notional_change: A dictionary of ASSET: Notional
        :return: One of SUPPORTED_STRATEGIES, e.g. SLOW, NORMAL, STEALTH
        """
        # 1. Check for SLOW conditions
        time_window = STRATEGY_PARAMETERS[STRATEGY_SLOW]["time_window"]
        client_volume_max = STRATEGY_PARAMETERS[STRATEGY_SLOW]["client_volume_max"]
        if self.recent_client_volume(time_window, hedger_time) < client_volume_max:
            return STRATEGY_SLOW

        # 2. Check for STEALTH conditions
        client_volume_trigger = STRATEGY_PARAMETERS[STRATEGY_STEALTH]["client_volume_trigger"]
        if sum(client_notional_change.values()) > client_volume_trigger:
            return STRATEGY_STEALTH

        return STRATEGY_NORMAL

    def get_hedge_quantities_stealth(self, current_positions: dict) -> List:
        """
        Gets the hedge quantities for STEALTH strategy
        :param current_positions: Dictionary of current positions
        :return: A list of hedges to go into the execution queue
        """
        stealth_hedges_by_asset = {}
        for asset, quantity in current_positions.items():
            stealth_hedges_by_asset[asset] = []
            chunks = STRATEGY_PARAMETERS[STRATEGY_STEALTH]["execution_chunks"]
            multiplier = -1 if quantity >= 0 else 1
            for chunk in range(0, chunks):
                hedge = {asset: multiplier*quantity/chunks}
                stealth_hedges_by_asset[asset].append(hedge)

        # Interleave the lists of hedges
        stealth_hedges = [val for tup in zip(*stealth_hedges_by_asset.values()) for val in tup]

        return stealth_hedges

    def get_hedge_quantities(self, current_positions: dict, client_position_change: dict,
                             strategy: SUPPORTED_STRATEGIES) -> dict:
        """
        Determine the best strategy for hedging and
        return the quantities to hedge
        :param current_positions: Dictionary of current positions
        :param client_position_change: Dictionary of client position change
        :param strategy: The hedging strategy in use
        :return: Dictionary of quantities to hedge
        """
        # Strategy descriptions:
        #
        # SLOW:
        # When there has not been much client activity within a recent window,
        # hold off on hedging to minimise transaction costs. Wait until total client
        # volume within a given window reaches a certain size to hedge. The hedger will
        # stay in SLOW mode and either switch to NORMAL or STEALTH, then the risk will
        # be hedged.
        #
        # NORMAL:
        # A simple strategy which will hedge the net total quantity for each asset.
        # This reduces transaction costs versus hedging every individual trade.
        #
        # STEALTH:
        # If we receive a huge amount of risk from a client in the latest batch
        # then we can do some more careful hedging in the market by chunking up
        # the position over time.
        if strategy not in SUPPORTED_STRATEGIES:
            raise Exception("Unknown hedging strategy " + strategy)

        # We could avoid this with match / case etc. in > Python 3.8
        hedge_quantities = Counter()
        if strategy == STRATEGY_NORMAL:
            self.log("Setting strategy to NORMAL")
            hedge_quantities = self.get_hedge_quantities_normal(client_position_change)
        elif strategy == STRATEGY_SLOW:
            self.log("Client activity is slow, setting strategy to SLOW")
        elif strategy == STRATEGY_STEALTH:
            self.log("Large position change, setting strategy to STEALTH")
            for hedge in self.get_hedge_quantities_stealth(current_positions):
                self.execution_queue.append(hedge)
        else:
            # Should not be able to get here
            raise Exception("Unimplemented strategy " + strategy)

        # If there are any hedges in the execution queue then pop
        # and include in quantities for hedging
        if len(self.execution_queue) > 0:
            hedge_quantities.update(self.execution_queue.pop(0))

        return hedge_quantities

    def place_hedge_orders(self, hedge_positions: dict, trade_time: datetime) -> None:
        """
        Use connected Exchange to place hedging orders in the market
        and then book those trades into the hedge book
        :param hedge_positions: A dictionary of ASSET: Position Quantity
        :param trade_time: Trade execution time
        :return: None
        """
        for asset, quantity in hedge_positions.items():
            self.log("Placing order for: " + str(quantity) + " " + asset)
            hedge_trade = self.exchange.at_market_order(self.hedge_book.name, asset, quantity, trade_time)
            self.hedge_book.trade_add(hedge_trade)

    def run_hedging(self) -> bool:
        """
        Central method for Hedger class.
        Run hedging strategies on client trades to build
        set of hedge trades
        :return: Returns True if successful, False otherwise
        """
        # Print some starting details
        hedger_time = self.start_time
        self.log("Starting up...")
        self.log_hedging_parameters()

        # Loop until time to stop
        while hedger_time <= self.end_time or len(self.execution_queue) > 0:
            # 0. Update times and log any start up details
            last_cycle_time = hedger_time
            hedger_time += timedelta(minutes=1)
            self.log("CYCLE BEGIN")
            self.log("Hedger time: " + str(hedger_time))
            self.log("Last cycle: " + str(last_cycle_time))

            # 1. Book any new client trades since last cycle
            # and get the client position change
            client_trades_booked = self.book_client_trades(last_cycle_time, hedger_time)
            client_position_change = self.trades_net_by_position(client_trades_booked)
            self.log("Client position change: " + str(client_position_change))

            # 2. Check current position
            current_positions = self.portfolio.net_positions()
            self.log("Current positions: " + str(current_positions))

            # 3. Decide current hedging strategy
            client_notional_change = self.trades_net_by_notional(client_trades_booked)
            current_strategy = self.get_current_strategy(hedger_time, client_notional_change)
            self.log(str("Current hedging strategy: " + current_strategy))

            # 4. Decide quantities to hedge
            hedge_quantities = self.get_hedge_quantities(current_positions, client_position_change, current_strategy)
            self.log(str("Hedge quantities: " + str(hedge_quantities)))

            # 5. Use connected Exchange to place hedging orders
            self.place_hedge_orders(hedge_quantities, hedger_time)
            self.log("Hedge orders booked. Position: " + str(self.portfolio.net_positions()))

            # 6. Anything else
            self.log("CYCLE END")
        return True

    def save_hedge_trades(self) -> None:
        """
        Saves the hedge trades to an output csv file
        """
        output_file = "hedge_trades.csv"
        self.log("Writing Hedge Trades to " + output_file)
        hedge_trades = self.hedge_book.get_trades()
        hedge_trades.to_csv(output_file)

    def run(self) -> None:
        """
        The main method to run the auto hedger
        """
        self.load_client_trades()
        self.run_hedging()
        self.save_hedge_trades()


def main():
    # Change this to your chosen local file path
    trades_file = "client_trades.csv"
    hedger = Hedger(trades_file)
    hedger.run()


if __name__ == "__main__":
    main()
