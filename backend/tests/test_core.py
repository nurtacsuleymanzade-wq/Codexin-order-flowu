import unittest

from app.core import LocalOrderBook, MarketState, SequenceGap
from app.intelligence import LiquidityIntelligenceEngine


class OrderBookTests(unittest.TestCase):
    def snapshot(self):
        return {"lastUpdateId": 100, "bids": [["100", "2"], ["99", "1"]], "asks": [["101", "2"], ["102", "1"]]}

    def test_snapshot_buffer_sequence_and_metrics(self):
        book = LocalOrderBook()
        book.buffer({"U": 99, "u": 101, "pu": 98, "b": [["100", "3"]], "a": []})
        book.buffer({"U": 102, "u": 103, "pu": 101, "b": [], "a": [["101", "0"]]})
        book.apply_buffered(self.snapshot())
        self.assertTrue(book.valid)
        self.assertEqual(book.last_update_id, 103)
        self.assertNotIn("101", book.asks)
        self.assertTrue(book.metrics()["spread"] > 0)

    def test_gap_invalidates_chain(self):
        book = LocalOrderBook()
        book.reset(self.snapshot())
        book.apply({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
        with self.assertRaises(SequenceGap):
            book.apply({"U": 102, "u": 103, "pu": 999, "b": [], "a": []})

    def test_snapshot_boundary_discards_stale_event(self):
        book = LocalOrderBook()
        book.buffer({"U": 100, "u": 100, "pu": 99, "b": [], "a": []})
        book.buffer({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
        book.apply_buffered(self.snapshot())
        self.assertTrue(book.valid)
        self.assertEqual(book.last_update_id, 101)

    def test_visible_totals_are_real_aggregated_levels(self):
        book = LocalOrderBook()
        book.reset(self.snapshot())
        book.apply({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
        metrics = book.metrics()
        self.assertEqual(metrics["bid_levels"], 2)
        self.assertEqual(metrics["ask_levels"], 2)
        self.assertAlmostEqual(metrics["bid_btc"], 3)
        self.assertAlmostEqual(metrics["ask_btc"], 3)
        self.assertLess(metrics["bids"][0]["price"], metrics["asks"][0]["price"])
        self.assertNotIn("unique_buyers", metrics)
        self.assertNotIn("unique_sellers", metrics)

    def test_depth_changes_expose_old_and_new_quantities(self):
        book = LocalOrderBook()
        book.reset(self.snapshot())
        changes = book.apply({"U": 101, "u": 101, "pu": 100, "b": [["100", "3"]], "a": [["101", "0"]]})
        self.assertEqual(changes[0]["side"], "BID")
        self.assertEqual(changes[0]["old_qty"], 2)
        self.assertEqual(changes[0]["new_qty"], 3)


class MarketStateTests(unittest.TestCase):
    def test_trade_metrics_have_event_and_receive_times(self):
        state = MarketState()
        event = state.trade({"e": "trade", "T": 1000, "t": 7, "p": "100", "q": "2", "m": False}, received_at=2000)
        self.assertEqual(event["event_time"], 1000)
        self.assertEqual(event["received_time"], 2000)
        self.assertEqual(state.cvd, 200)
        self.assertEqual(state.trade_count, 1)


class LiquidityIntelligenceTests(unittest.TestCase):
    def seeded(self):
        book = LocalOrderBook()
        book.reset({"lastUpdateId": 100, "bids": [["99", "20"]], "asks": [["101", "100"]]})
        book.apply({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
        book.valid = True
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        return book, engine

    def test_execution_reconciliation_is_separate_from_cancelled(self):
        book, engine = self.seeded()
        engine.on_trade({"price": 101, "quantity": 2, "notional": 202, "side": "BUY"}, 1100)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 98}], 1200, 100, 1)
        wall = engine.walls["ASK:101.00000000"]
        self.assertAlmostEqual(wall.estimated_executed_qty, 2)
        self.assertLess(wall.estimated_cancelled_qty, 0.1)

    def test_pull_and_replenishment_have_distinct_evidence(self):
        book, engine = self.seeded()
        # A recent opposite-side trade is only a feed-coverage heartbeat; it
        # cannot reconcile against ask depletion.  This lets unmatched depth
        # removal be classified as cancellation evidence rather than UNKNOWN.
        engine.on_trade({"price": 100, "quantity": .01, "notional": 1, "side": "SELL"}, 1100)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 80}], 1200, 100, 1)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 80, "new_qty": 100}], 1300, 100, 1)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 0}], 1400, 100, 1)
        wall = engine.walls["ASK:101.00000000"]
        self.assertGreaterEqual(wall.replenishment_count, 1)
        self.assertEqual(wall.pull_count, 1)
        self.assertGreater(wall.pull_score, 0)

    def test_indicator_output_and_probability_guard(self):
        _, engine = self.seeded()
        bars = [{"time": index * 60, "open": 100 + index * .1, "high": 100.2 + index * .1, "low": 99.8 + index * .1, "close": 100 + index * .1, "volume": 10, "closed": True} for index in range(40)]
        out = engine.snapshot(engine_book := self.seeded()[0], bars, [{"time": index * 60, "buy": 1000, "sell": 700} for index in range(40)], 104, 3000)
        self.assertIn("rsi", out["indicators"])
        self.assertIn("macd", out["indicators"])
        self.assertEqual(out["probability_status"], "UNCALIBRATED")
        self.assertTrue(all(target["p_touch"]["1m"] is None for target in out["targets"]))


if __name__ == "__main__":
    unittest.main()
