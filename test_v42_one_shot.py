import unittest, tempfile
from pathlib import Path
from unittest.mock import patch
import v42_one_shot as m

class OneShotTests(unittest.TestCase):
    def test_plan(self):
        d=m.execute(['--symbols','BTC-USDT','ETH-USDT','--start','2026-06-01T00:00:00Z','--end','2026-06-08T00:00:00Z','--plan-only'])
        self.assertEqual(d['expected_1m_bars_per_symbol'],10080)
    def test_duplicate(self):
        with self.assertRaises(ValueError):m.execute(['--symbols','BTC-USDT','BTC-USDT','--start','2026-06-01T00:00:00Z','--end','2026-06-08T00:00:00Z','--plan-only'])
    def test_future(self):
        with self.assertRaises(ValueError):m.execute(['--symbols','BTC-USDT','--start','2030-06-01T00:00:00Z','--end','2030-06-08T00:00:00Z','--plan-only'])
    def test_short(self):
        with self.assertRaises(ValueError):m.execute(['--symbols','BTC-USDT','--start','2026-06-01T00:00:00Z','--end','2026-06-02T00:00:00Z','--plan-only'])
    def test_plan_makes_no_files(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'nothing'
            m.execute(['--symbols','BTC-USDT','--start','2026-06-01T00:00:00Z','--end','2026-06-08T00:00:00Z','--workdir',str(p),'--plan-only'])
            self.assertFalse(p.exists())
if __name__=='__main__':unittest.main()
