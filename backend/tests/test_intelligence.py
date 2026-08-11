import time
import unittest

from app.core import LocalOrderBook
from app.intelligence import LiquidityIntelligenceEngine


def seeded_book(bid_qty=25.0, ask_qty=100.0):
    book = LocalOrderBook()
    book.reset({"lastUpdateId": 100, "bids": [["99", str(bid_qty)]], "asks": [["101", str(ask_qty)]]})
    book.apply({"U": 101, "u": 101, "pu": 100, "b": [], "a": []})
    return book


class LifecycleScenarios(unittest.TestCase):
    def test_unmatched_depth_without_trade_coverage_stays_unknown(self):
        book = seeded_book()
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 70}], 1200, 100, 1)
        wall = engine.walls[engine._key("ASK", 101)]
        self.assertEqual(wall.estimated_executed_qty, 0)
        self.assertEqual(wall.estimated_cancelled_qty, 0)
        self.assertAlmostEqual(wall.unknown_removed_qty, 30)

    def test_scenario_a_pull_before_touch(self):
        book = seeded_book(25, 300)
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 98)
        engine.on_trade({"price": 101, "quantity": 10, "notional": 1010, "side": "BUY"}, 1200)
        engine.on_price(100.7, 1250)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 300, "new_qty": 10}], 1300, 100.7, 1)
        wall = engine.walls[engine._key("ASK", 101)]
        self.assertAlmostEqual(wall.estimated_executed_qty, 10)
        self.assertGreater(wall.estimated_cancelled_qty, 250)
        self.assertGreaterEqual(wall.pull_count, 1)
        self.assertGreaterEqual(wall.pull_score, 70)
        self.assertEqual(wall.role, "SPOOF_LIKE")

    def test_scenario_b_replenishment_absorption_and_iceberg(self):
        book = seeded_book(25, 100)
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        timestamp = 1100
        for _ in range(3):
            engine.on_trade({"price": 101, "quantity": 50, "notional": 5050, "side": "BUY"}, timestamp)
            engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 50}], timestamp + 20, 100.9, 1)
            engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 50, "new_qty": 100}], timestamp + 80, 100.9, 1)
            timestamp += 150
        wall = engine.walls[engine._key("ASK", 101)]
        self.assertEqual(wall.replenishment_count, 3)
        self.assertAlmostEqual(wall.estimated_executed_qty, 150)
        self.assertGreater(wall.replenishment_score, 60)
        self.assertGreater(wall.absorption_score, 50)
        self.assertGreater(wall.iceberg_score, 50)
        self.assertGreater(wall.hidden_liquidity_estimate, 0)
        self.assertTrue(any(event["event_type"] == "REPLENISHMENT_EVENT" for event in engine.events))

    def test_migration_is_pattern_match_not_order_identity(self):
        book = seeded_book(25, 100)
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        engine.on_trade({"price": 100, "quantity": .01, "notional": 1, "side": "SELL"}, 1100)
        engine.on_depth([{"side": "ASK", "price": 101, "old_qty": 100, "new_qty": 0}], 1200, 100, 2)
        engine.on_depth([{"side": "ASK", "price": 100.5, "old_qty": 0, "new_qty": 95}], 1300, 100, 2)
        moved = engine.walls[engine._key("ASK", 100.5)]
        self.assertGreater(moved.migration_confidence, .65)
        self.assertEqual(moved.migration_direction, "TOWARD_PRICE")
        event = next(item for item in engine.migration_events if item["side"] == "ASK")
        self.assertIn("NO ORDER_ID", event["inference"])


class MicrostructureScenarios(unittest.TestCase):
    def test_microprice_multiband_obi_and_vacuum(self):
        book = LocalOrderBook()
        book.reset({"lastUpdateId": 10, "bids": [["100", "10"], ["99", "3"]], "asks": [["101", "2"], ["102", ".1"], ["103", ".1"]]})
        book.apply({"U": 11, "u": 11, "pu": 10, "b": [], "a": []})
        engine = LiquidityIntelligenceEngine()
        engine.last_atr = 4
        engine.reset_from_book(book, 1000, 100.5)
        result = engine.snapshot(book, [], [], 100.5, 2000)
        self.assertGreater(result["microstructure"]["microprice"], result["microstructure"]["mid_price"])
        self.assertIn("top50", result["microstructure"]["obi"])
        self.assertGreater(result["detectors"]["vacuum"]["up"], result["detectors"]["vacuum"]["down"])

    def test_lri_target_and_touch_break_are_separate(self):
        book = LocalOrderBook()
        book.reset({"lastUpdateId": 10, "bids": [["100", "10"], ["99", "4"]], "asks": [["101", ".1"], ["102", ".2"], ["103", "40"]]})
        book.apply({"U": 11, "u": 11, "pu": 10, "b": [], "a": []})
        engine = LiquidityIntelligenceEngine()
        engine.last_atr = 4
        engine.reset_from_book(book, 1000, 100.5)
        engine.on_trade({"price": 101, "quantity": 5, "notional": 505, "side": "BUY"}, 1900)
        result = engine.snapshot(book, [], [], 100.5, 2000)
        self.assertTrue(result["targets"])
        target = result["targets"][0]
        self.assertIn("lri", target)
        self.assertIsNone(target["p_touch"]["1m"])
        self.assertIsNone(target["p_break_given_touch"])
        self.assertNotEqual(target["touch_score"], target["break_score"])
        self.assertEqual(target["probability_status"], "UNCALIBRATED")

    def test_timeframe_delta_uses_current_selected_bucket(self):
        engine = LiquidityIntelligenceEngine()
        buckets = [
            {"time": 0, "buy": 100, "sell": 10, "count": 1},
            {"time": 60_000, "buy": 50, "sell": 20, "count": 1},
            {"time": 120_000, "buy": 10, "sell": 30, "count": 1},
            {"time": 180_000, "buy": 40, "sell": 10, "count": 1},
            {"time": 240_000, "buy": 20, "sell": 5, "count": 1},
        ]
        one = engine.indicators([], buckets, "1m")
        five = engine.indicators([], buckets, "5m")
        self.assertEqual(one["delta"], 15)
        self.assertEqual(five["delta"], 145)

    def test_invalid_sequence_suppresses_all_intelligence_outputs(self):
        book = seeded_book()
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        book.invalidate()
        result = engine.snapshot(book, [], [], 100, 2000, data_integrity_ok=False)
        self.assertFalse(result["trusted"])
        self.assertEqual(result["health_state"], "DEGRADED")
        self.assertEqual(result["walls"], [])
        self.assertEqual(result["targets"], [])

    def test_depth_hot_path_is_bounded(self):
        book = seeded_book()
        engine = LiquidityIntelligenceEngine()
        engine.reset_from_book(book, 1000, 100)
        changes = [{"side": "ASK", "price": 101 + index * .1, "old_qty": 0, "new_qty": 1 + index % 10} for index in range(100)]
        started = time.perf_counter()
        engine.on_depth(changes, 1200, 100, 2)
        self.assertLess((time.perf_counter() - started) * 1000, 500)
        self.assertLessEqual(len(engine.walls), 650)


if __name__ == "__main__":
    unittest.main()
