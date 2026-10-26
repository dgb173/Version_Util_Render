import unittest,copy
from demo import fixture
from evaluate import evaluate


class EvaluationTests(unittest.TestCase):
    def record(self):
        s=fixture();r=dict(event_id=s['event_id'],home_goals=2,away_goals=1,available_at='2026-10-26T20:10:00Z',source_ref='DEMO:RESULT')
        return dict(snapshot=s,result=r)

    def test_units_and_two_markets_one_event(self):
        out=evaluate([self.record()]);self.assertEqual(out['n_events'],1)
        self.assertEqual(out['quoted_picks'],2);self.assertAlmostEqual(out['paper_roi'],(.72+.83)/2)
        self.assertIsNone(out['event_bootstrap_95'])

    def test_no_quotes_no_roi(self):
        r=self.record();r['snapshot']['quotes']=[];out=evaluate([r])
        self.assertIsNone(out['paper_roi']);self.assertEqual(out['quoted_picks'],0)

    def test_duplicate_event_rejected(self):
        with self.assertRaises(ValueError):evaluate([self.record(),self.record()])

    def test_cluster_bootstrap_reproducible(self):
        a=self.record();b=copy.deepcopy(a);b['snapshot']['event_id']='DEMO2';b['result'].update(event_id='DEMO2',home_goals=0,away_goals=0)
        self.assertEqual(evaluate([a,b],bootstrap=200)['event_bootstrap_95'],evaluate([a,b],bootstrap=200)['event_bootstrap_95'])
