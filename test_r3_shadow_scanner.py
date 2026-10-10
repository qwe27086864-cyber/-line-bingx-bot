import io
import json
import unittest
from r3_shadow_scanner import evaluate_candidate, process_stream


class ShadowScannerTests(unittest.TestCase):
    def test_long_trend_extended(self):
        result = evaluate_candidate({"symbol":"ABC-USDT", "side":"LONG", "strategy":"TREND", "score":90,
                                     "price":103,"ema20_5m":100,"rsi5":60,
                                     "btc_change_5m_pct":0.1,"eth_change_5m_pct":0.1})
        self.assertEqual(result["mode"],"SHADOW_ONLY_NO_TRADING")
        self.assertTrue(result["assessment"]["would_block"])
        self.assertIn("LONG_EMA20_OVEREXTENDED",result["assessment"]["reasons"])

    def test_normal_long_trend(self):
        result=evaluate_candidate({"side":"LONG","strategy":"TREND","price":101,
                                   "ema20_5m":100,"rsi5":60,"btc_change_5m_pct":0.1,"eth_change_5m_pct":0.1})
        self.assertFalse(result["assessment"]["would_block"])

    def test_missing_is_blocked(self):
        result=evaluate_candidate({"side":"LONG","strategy":"TREND"})
        self.assertTrue(result["assessment"]["would_block"])

    def test_other_strategy_not_changed(self):
        result=evaluate_candidate({"side":"SHORT","strategy":"BREAKOUT"})
        self.assertFalse(result["assessment"]["applicable"])

    def test_stream_invalid(self):
        source=io.StringIO('{"side":"LONG","strategy":"TREND"}\nnot json\n')
        target=io.StringIO()
        stats=process_stream(source,target)
        self.assertEqual(stats["read"],1)
        self.assertEqual(stats["invalid"],1)
        self.assertEqual(len(target.getvalue().splitlines()),1)

    def test_no_input_mutation(self):
        candidate={"side":"LONG","strategy":"TREND","price":100}
        original=dict(candidate)
        evaluate_candidate(candidate)
        self.assertEqual(candidate,original)

if __name__=='__main__':
    unittest.main()
