import unittest

from app.core import MarketState
from app.decision_brain import DecisionBrain, structure
from app.telegram import TelegramReporter


class DecisionBrainContractTests(unittest.TestCase):
    def valid_book(self):
        state = MarketState()
        state.price = 100.0
        state.orderbook.reset({"lastUpdateId": 100, "bids": [["99", "10"]], "asks": [["101", "10"]]})
        state.orderbook.apply({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
        state.last_book_at = 10_000
        return state

    def test_one_second_flow_cannot_create_direction_without_one_minute_setup(self):
        state = self.valid_book()
        for index in range(5):
            state.trade({"T": 9_500 + index * 10, "p": "100.1", "q": "1", "m": False}, received_at=9_500 + index * 10)
        state.last_trade_at = 9_990
        decision = DecisionBrain().evaluate(state, 10_000)
        self.assertEqual(decision["final_decision"], "NO TRADE")
        self.assertEqual(decision["one_second_confirmation"]["status"], "NOT CONFIRMED")
        self.assertEqual(decision["one_second_confirmation"]["evidence"][0]["name"], "1M setup required")

    def test_missing_structure_is_explicit(self):
        self.assertEqual(structure([])["label"], "N/A")
        self.assertEqual(structure([])["status"], "INSUFFICIENT DATA")

    def test_timeframes_are_separate(self):
        state = MarketState()
        row = {"time": 1, "open": 1, "high": 2, "low": 0, "close": 1, "volume": 1, "closed": True}
        state.update_kline(row, timeframe="1m")
        state.update_kline({**row, "time": 5}, timeframe="5m")
        state.update_kline({**row, "time": 15}, timeframe="15m")
        self.assertEqual(len(state.timeframe_klines["1m"]), 1)
        self.assertEqual(len(state.timeframe_klines["5m"]), 1)
        self.assertEqual(len(state.timeframe_klines["15m"]), 1)

    def test_estimated_liquidation_is_not_observed(self):
        state = self.valid_book()
        state.open_interest = 1000
        state.liquidations.append({"price": 102, "notional": 500, "side": "BUY"})
        output = DecisionBrain._liquidations(state, 100, {"5m": {}}, {"volume_total": 10}, {"value_area": {}})
        self.assertEqual(output["status"], "ESTIMATED")
        self.assertTrue(all(item["status"] == "ESTIMATED" for item in output["estimated"]))
        self.assertEqual(len(output["observed"]), 1)
        self.assertNotEqual(output["observed"][0].get("status"), "ESTIMATED")


class TelegramContractTests(unittest.TestCase):
    def test_credentials_are_required_and_not_reported(self):
        reporter = TelegramReporter(None, None)
        status = reporter.status()
        self.assertFalse(status["enabled"])
        self.assertNotIn("token", status)


if __name__ == "__main__":
    unittest.main()
