"""
test_autohedger.py:
    Test file for autohedger.py (A Basic Auto Hedger process for Spot Bitcoin and Ethereum.)
    See README file for more details.

Author: Matt Webb
Version: 1.0
"""

__author__ = "Matt Webb"
__version__ = "1.0"

import unittest
from unittest.mock import patch

import autohedger
from autohedger import Hedger


class test_autohedger(unittest.TestCase):
    """
    Test class for autohedger.
    Exchange.get_close_prices() is mocked to avoid calls to Yahoo API.
    """
    @patch("autohedger.Exchange.get_close_prices", return_value={"BTC": 30000.00, "ETH": 1800.00})
    def test_hedger_strategies(self, client_trades_file):
        """
        Tests that the given strategy gets triggered
        under the right client trade conditions.
        """
        test_cases = {
            "test_stealth.csv": autohedger.STRATEGY_STEALTH,
            "test_normal_upper.csv": autohedger.STRATEGY_NORMAL,
            "test_normal_lower.csv": autohedger.STRATEGY_NORMAL,
            "test_slow.csv": autohedger.STRATEGY_SLOW
        }
        for test_file, expected_strategy in test_cases.items():
            # 1. Initialise and load test file
            hedger = Hedger(test_file)
            hedger.load_client_trades()

            # 2. Book the client trades and determine strategy
            client_trades_booked = hedger.book_client_trades(hedger.start_time, hedger.end_time)
            client_notional_change = hedger.trades_net_by_notional(client_trades_booked)
            current_strategy = hedger.get_current_strategy(hedger.end_time, client_notional_change)

            # 3. Log some details for transparency
            hedger.log("Client trades booked: " + str(client_trades_booked))
            hedger.log("Client notional change: " + str(client_notional_change))
            hedger.log("Resulting strategy: " + current_strategy)

            # 4. Check strategy is what we expect
            self.assertEqual(current_strategy, expected_strategy)

    def test_hedger_bad_strategy(self):
        """
        Tests Hedger.get_hedge_quantities
        """
        hedger = Hedger("test_stealth.csv")
        with self.assertRaises(Exception) as context:
            hedger.get_hedge_quantities({}, {}, "BAD")
        self.assertTrue("Unknown hedging strategy" in str(context.exception))


if __name__ == '__main__':
    unittest.main()
