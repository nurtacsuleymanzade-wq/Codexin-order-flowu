import unittest

from app.core import LocalOrderBook, MarketState, SequenceGap


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


class MarketStateTests(unittest.TestCase):
    def test_trade_metrics_have_event_and_receive_times(self):
        state = MarketState()
        event = state.trade({"e": "trade", "T": 1000, "t": 7, "p": "100", "q": "2", "m": False}, received_at=2000)
        self.assertEqual(event["event_time"], 1000)
        self.assertEqual(event["received_time"], 2000)
        self.assertEqual(state.cvd, 200)
        self.assertEqual(state.trade_count, 1)


if __name__ == "__main__":
    unittest.main()
