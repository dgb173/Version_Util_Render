import copy,json,sqlite3,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from contracts import Snapshot,Config
from market import quarters,strength_q,settle,legs
from engine import analyze,profile,blend,shot_projection
from demo import fixture
from postmatch import attach
from audit_log import digest,append_record
from review_gemini import validate_review,call_gemini


class MarketTests(unittest.TestCase):
    def test_exact_quarters(self):
        for q in range(-24,25): self.assertEqual(quarters(q/4),q)
        for bad in (.3,float('nan'),float('inf')):
            with self.assertRaises(ValueError):quarters(bad)

    def test_nowgoal_perspective(self):
        self.assertEqual(strength_q(.75,'home'),3)
        self.assertEqual(strength_q(.75,'away'),-3)
        self.assertEqual(strength_q(-1.5,'away'),6)

    def test_negative_split(self):
        self.assertEqual(legs(-3),(-4,-2))
        self.assertEqual(legs(-1),(-2,0))

    def test_half_and_full_payments(self):
        cases=[('AH_HOME',-3,1,0,'HALF_WIN',.45),('AH_HOME',-1,1,1,'HALF_LOSS',-.5),
               ('AH_AWAY',1,1,1,'HALF_WIN',.45),('UNDER',11,2,1,'HALF_LOSS',-.5),
               ('UNDER',12,2,1,'PUSH',0),('OVER',11,2,1,'HALF_WIN',.45),
               ('OVER',10,2,1,'WIN',.9),('OVER',9,1,1,'HALF_LOSS',-.5)]
        for m,q,h,a,status,p in cases:
            with self.subTest(market=m,line=q):
                out=settle(m,q,h,a,1.9);self.assertEqual(out['status'],status);self.assertAlmostEqual(out['profit_per_unit'],p)

    def test_opposite_markets_zero_sum_at_even_odds(self):
        for q in range(-9,10):
            for h in range(6):
                for a in range(6):
                    self.assertAlmostEqual(settle('AH_HOME',q,h,a)['profit_per_unit']+settle('AH_AWAY',-q,h,a)['profit_per_unit'],0)
        for q in range(1,18):
            for t in range(8):
                self.assertEqual(settle('OVER',q,t,0)['profit_per_unit']+settle('UNDER',q,t,0)['profit_per_unit'],0)


class DataTests(unittest.TestCase):
    def test_zero_sample_not_zero_cover(self):
        s=fixture();s['similar_current']['away'].update(n=0,sample_ids=[],cover_rate=None,gf=None,ga=None,losses=None)
        out=analyze(s);self.assertFalse(out['b'][0]['available'])
        s['similar_current']['away']['cover_rate']=0
        with self.assertRaises(ValueError):analyze(s)

    def test_duplicate_history_rejected(self):
        s=fixture();s['history'].append(copy.deepcopy(s['history'][0]))
        with self.assertRaises(ValueError):analyze(s)

    def test_future_history_rejected(self):
        s=fixture();s['history'][0]['available_at']='2026-10-27T01:00:00Z'
        with self.assertRaises(ValueError):analyze(s)

    def test_unknown_extra_input_rejected(self):
        s=fixture();s['final_result']='2:1'
        with self.assertRaises(ValueError):analyze(s)

    def test_zero_and_missing_sot_different(self):
        s=fixture();s['history'][0].update(gf=0,shot_goals_for=0,sot_for=0)
        a=analyze(s)['profiles']['favorite']['active']
        s['history'][0]['sot_for']=None
        b=analyze(s)['profiles']['favorite']['active']
        self.assertEqual(a['counts']['sot_for'],5);self.assertEqual(b['counts']['sot_for'],4)
        self.assertNotEqual(a['sot_for'],b['sot_for'])

    def test_impossible_telemetry_rejected(self):
        s=fixture();s['history'][0]['sot_for']=100
        with self.assertRaises(ValueError):analyze(s)

    def test_unknown_shot_goals_not_replaced_by_score(self):
        s=fixture()
        for r in s['history']:r['shot_goals_for']=None
        out=analyze(s);self.assertIsNone(out['shot_projection'])
        self.assertIsNone(out['profiles']['favorite']['active']['conversion_for'])

    def test_ratio_of_sums(self):
        s=Snapshot.model_validate(fixture());rows=[s.history[0].model_copy(update={'sot_for':1,'shot_goals_for':1}),
                                               s.history[1].model_copy(update={'sot_for':9,'shot_goals_for':1})]
        p=profile(rows,Config(),.3);self.assertAlmostEqual(p['conversion_for'],.2)
        self.assertNotAlmostEqual(p['conversion_for'],(1+1/9)/2)

    def test_role_and_reserve_filter(self):
        s=fixture();s['history'][0]['opponent_squad']='reserve';s['history'][1]['raw_home_ah']=-1
        reasons={x['reason'] for x in analyze(s)['exclusions']['favorite']}
        self.assertTrue({'reserve_or_u20','role_shift'}<=reasons)

    def test_references_deduplicated(self):
        s=fixture();old=analyze(s)['g'][1]['values'];s['references'].append(copy.deepcopy(s['references'][0]))
        self.assertEqual(analyze(s)['g'][1]['values'],old)

    def test_overlapping_similar_not_counted_twice(self):
        s=fixture();s['similar_h2h']['home']['sample_ids']=s['similar_current']['home']['sample_ids'][:]
        g=analyze(s)['g'][2];self.assertIsNone(g['values']['fused_q']);self.assertTrue(g['values']['overlap_blocks'])

    def test_explicit_blend_weights(self):
        a={k:4 for k in ('sot_for','sot_against','conversion_for_shrunk','conversion_against_shrunk')}
        b={k:2 for k in a};out=blend(a,b,Config())
        self.assertAlmostEqual(out['values']['sot_for'],3.3)
        b['sot_for']=None;out=blend(a,b,Config());self.assertEqual(out['actual_active_weights']['sot_for'],1)

    def test_rule_of_three(self):
        f={'sot_for':6,'sot_against':3,'conversion_for_shrunk':.3,'conversion_against_shrunk':.2}
        d={'sot_for':4,'sot_against':5,'conversion_for_shrunk':.25,'conversion_against_shrunk':.35}
        p=shot_projection(f,d,4.5)
        self.assertAlmostEqual(p['favorite']['sot'],6*5/4.5)
        self.assertAlmostEqual(p['favorite']['goals_proxy'],6*5/4.5*.325)


class DecisionTests(unittest.TestCase):
    def test_complete_demo_vectors(self):
        out=analyze(fixture())
        self.assertEqual([g['bit'] for g in out['b']],[1,1,1,0,1,1])
        self.assertEqual([g['bit'] for g in out['g']],[1,1,1,0,1,1])
        self.assertEqual(out['ah_pick']['line_q'],-2);self.assertEqual(out['ou_pick']['line_q'],10)

    def test_partial_data_keeps_direction_and_mask(self):
        out=analyze(fixture('datos_parciales'))
        self.assertFalse(out['b'][0]['available']);self.assertEqual(out['b'][0]['bit'],0)
        self.assertEqual(out['score_ah']['valid'],3)
        self.assertIn(out['ou_pick']['market'],('OVER','UNDER'))
        self.assertIsNone(out['ah_pick']['quote'])

    def test_b1_conflict_dog_priority(self):
        s=fixture();s['similar_current']['away'].update(cover_rate=.60,losses=0)
        self.assertEqual(analyze(s)['b'][0]['bit'],0)

    def test_shot_gate_boundary(self):
        s=fixture()
        for i,r in enumerate(s['history'][:5]):
            r['sot_for']=[6,6,5,5,None][i];r['ap_for']=55
        self.assertEqual(analyze(s)['b'][4]['bit'],1)  # 5.5 - 4 = 1.5; 55 - 45 = 10
        s['history'][0]['sot_for']=5
        self.assertEqual(analyze(s)['b'][4]['bit'],0)

    def test_lock_overrides_reference_over(self):
        s=fixture('cerrojo')
        for r in s['references']:r['total_goals']=5
        out=analyze(s);self.assertEqual(out['ou_pick']['market'],'UNDER')
        self.assertIn('CONFLICT_LOCK_OVERRIDES_OVER',out['policy_trace'])

    def test_long_draw_veto(self):
        out=analyze(fixture('linea_larga'))
        self.assertEqual(out['ah_pick']['market'],'AH_AWAY');self.assertEqual(out['ah_pick']['line_q'],6)

    def test_unilateral_negative_delta_not_favorite_support(self):
        s=fixture()
        for r in s['history']:
            if r['team_id']=='A':r['raw_home_ah']=0
            else:r['raw_home_ah']=-1.5
        # Widen only the role filter; the sign is still unfavorable to favorite.
        out=analyze(s,{'role_tolerance_q':20})
        self.assertLess(out['g'][3]['values']['delta_q'],0);self.assertFalse(out['g'][3]['values']['unilateral'])

    def test_mirror_path(self):
        s=fixture();s['bridges']=[dict(match_id='MIRROR',total_goals=3,home_goals=2,away_goals=1,
             home_id='R0',away_id='R1',competition_id='DEMO',raw_home_ah=.5,
             kickoff='2026-10-10T12:00:00Z',available_at='2026-10-10T15:00:00Z',source_ref='DEMO:MIRROR')]
        pairs=analyze(s)['b'][2]['values']['pairs'];self.assertTrue(any(p['kind']=='mirror_path' for p in pairs))

    def test_away_favorite_sign(self):
        s=fixture();s['raw_home_ah']=-.75
        out=analyze(s,{'role_tolerance_q':12})
        self.assertEqual(out['favorite_id'],'B');self.assertEqual(out['canonical_home_line_q'],3)

    def test_pickem_explicit_anchor(self):
        s=fixture();s['raw_home_ah']=0
        out=analyze(s,{'role_tolerance_q':10});self.assertTrue(out['pickem_anchor'])

    def test_quote_not_invented(self):
        s=fixture();s['quotes']=[]
        out=analyze(s);self.assertEqual(out['ou_pick']['quote_status'],'TARGET_UNQUOTED')
        self.assertIsNone(out['ou_pick']['expected_value'])


class FeedbackTests(unittest.TestCase):
    def packet(self):
        s=Snapshot.model_validate(fixture());a=analyze(s)
        return dict(phase='prematch',synthetic=True,snapshot=s.model_dump(mode='json'),analysis=a,evidence_refs=a['evidence_refs'])

    def test_postmatch_frozen_and_settled(self):
        p=self.packet();before=digest(p)
        out=attach(p,dict(event_id=p['snapshot']['event_id'],home_goals=2,away_goals=1,
                        available_at='2026-10-26T20:00:00Z',source_ref='DEMO:RESULT'))
        self.assertEqual(digest(p),before);self.assertEqual(out['prematch_sha256'],before)
        self.assertAlmostEqual(out['settlements']['ou_pick']['profit_per_unit'],.83)

    def test_unquoted_profit_not_fabricated(self):
        p=self.packet();p['analysis']['ah_pick']['quote']=None
        out=attach(p,dict(event_id=p['snapshot']['event_id'],home_goals=2,away_goals=1,
                        available_at='2026-10-26T20:00:00Z',source_ref='DEMO:RESULT'))
        self.assertIsNone(out['settlements']['ah_pick']['profit_per_unit'])

    def test_wrong_event_rejected(self):
        with self.assertRaises(ValueError):
            attach(self.packet(),dict(event_id='WRONG',home_goals=2,away_goals=1,
                                     available_at='2026-10-26T20:00:00Z',source_ref='DEMO:RESULT'))

    def test_gemini_mock_validates_hash_and_references(self):
        p=self.packet();r=dict(packet_sha256=digest(p),verdict='ready_for_validation',summary='Prueba simulada',issues=[],hypotheses=[],limitations=['Sintético'])
        client=SimpleNamespace(interactions=SimpleNamespace(create=lambda **kw:SimpleNamespace(output_text=json.dumps(r))))
        out,_=call_gemini(p,client,'mock');self.assertEqual(out.packet_sha256,digest(p))
        r['packet_sha256']='bad'
        with self.assertRaises(ValueError):validate_review(json.dumps(r),p)
        r['packet_sha256']=digest(p);r['issues']=[dict(severity='warning',category='data',statement='x',evidence_refs=['INVENTADA'],suggested_test='x')]
        with self.assertRaises(ValueError):validate_review(json.dumps(r),p)

    def test_append_only_ledger(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'audit.sqlite';append_record(path,{'x':1})
            db = sqlite3.connect(path)
            try:
                with self.assertRaises(sqlite3.IntegrityError):db.execute('UPDATE audit SET payload=?',('{}',))
            finally:
                db.close()
            with self.assertRaises(sqlite3.IntegrityError):append_record(path,{'x':1})


if __name__=='__main__':unittest.main()
