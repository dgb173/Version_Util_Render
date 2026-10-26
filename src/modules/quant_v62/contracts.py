"""Point-in-time input contract. Missing telemetry is None, never zero."""
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
try:
    from .market import quarters
except ImportError:
    from market import quarters


class Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class PastMatch(Strict):
    match_id: str
    team_id: str
    opponent_id: str
    competition_id: str
    venue: Literal['home','away']
    opponent_squad: Literal['senior','reserve','u20'] = 'senior'
    kickoff: datetime
    available_at: datetime
    source_ref: str
    raw_home_ah: float | None = None
    raw_home_ah_close: float | None = None
    ou_line: float | None = Field(default=None, ge=0)
    gf: int = Field(ge=0, strict=True)
    ga: int = Field(ge=0, strict=True)
    # Goals attributed to a shot; own goals must be excluded by the adapter.
    shot_goals_for: int | None = Field(default=None, ge=0, strict=True)
    shot_goals_against: int | None = Field(default=None, ge=0, strict=True)
    shots_for: int | None = Field(default=None, ge=0, strict=True)
    shots_against: int | None = Field(default=None, ge=0, strict=True)
    sot_for: int | None = Field(default=None, ge=0, strict=True)
    sot_against: int | None = Field(default=None, ge=0, strict=True)
    attacks_for: int | None = Field(default=None, ge=0, strict=True)
    attacks_against: int | None = Field(default=None, ge=0, strict=True)
    ap_for: int | None = Field(default=None, ge=0, strict=True)
    ap_against: int | None = Field(default=None, ge=0, strict=True)
    corners_for: int | None = Field(default=None, ge=0, strict=True)
    corners_against: int | None = Field(default=None, ge=0, strict=True)
    red_card_context: bool = False

    @model_validator(mode='after')
    def consistency(self):
        for v in (self.raw_home_ah, self.raw_home_ah_close, self.ou_line):
            if v is not None: quarters(v)
        for side, g in [('for',self.gf),('against',self.ga)]:
            shots, sot, sg = (getattr(self,f'{k}_{side}') for k in ('shots','sot','shot_goals'))
            if shots is not None and sot is not None and sot > shots:
                raise ValueError('Tiros a puerta mayores que tiros totales')
            if sg is not None and (sg > g or (sot is not None and sg > sot)):
                raise ValueError('Goles de tiro incompatibles con goles/SOT')
        return self


class Similar(Strict):
    n: int = Field(ge=0, strict=True)
    cover_rate: float | None = Field(default=None, ge=0, le=1)
    losses: int | None = Field(default=None, ge=0, strict=True)
    gf: float | None = Field(default=None, ge=0)
    ga: float | None = Field(default=None, ge=0)
    sample_ids: list[str]
    cover_definition: str = 'platform_reported'

    @model_validator(mode='after')
    def counts(self):
        if len(set(self.sample_ids)) != self.n or len(self.sample_ids) != self.n:
            raise ValueError('N e IDs únicos del bloque no coinciden')
        if self.losses is not None and self.losses > self.n:
            raise ValueError('Derrotas mayores que N')
        if self.n == 0 and any(x is not None for x in (self.cover_rate,self.gf,self.ga,self.losses)):
            raise ValueError('Con N=0 usar null, no 0%')
        return self


class SimilarBlock(Strict):
    home: Similar
    away: Similar
    correlation: Literal['home','away','balanced','unknown'] = 'unknown'
    available_at: datetime
    source_ref: str


class Reference(Strict):
    match_id: str
    total_goals: int = Field(ge=0, strict=True)
    kickoff: datetime
    available_at: datetime
    source_ref: str


class Quote(Strict):
    market: Literal['AH_HOME','AH_AWAY','OVER','UNDER']
    line_q: int = Field(strict=True)
    odds: float = Field(gt=1)
    available_at: datetime
    bookmaker: str
    source_ref: str


class Bridge(Reference):
    home_id: str
    away_id: str
    competition_id: str
    raw_home_ah: float
    home_goals: int = Field(ge=0,strict=True)
    away_goals: int = Field(ge=0,strict=True)

    @model_validator(mode='after')
    def valid_bridge(self):
        quarters(self.raw_home_ah)
        if self.home_id==self.away_id or self.total_goals!=self.home_goals+self.away_goals:
            raise ValueError('Puente espejo inconsistente')
        return self


class LeagueRecord(Strict):
    wins: int = Field(ge=0,strict=True)
    draws: int = Field(ge=0,strict=True)
    losses: int = Field(ge=0,strict=True)


class Snapshot(Strict):
    schema_version: Literal['6.2-binary'] = '6.2-binary'
    event_id: str
    home_id: str
    away_id: str
    competition_id: str
    decision_at: datetime
    kickoff: datetime
    raw_home_ah: float
    ou_line: float = Field(ge=.5)
    rank_home: int | None = Field(default=None, ge=1)
    rank_away: int | None = Field(default=None, ge=1)
    record_home: LeagueRecord | None = None
    record_away: LeagueRecord | None = None
    senior_match: bool = True
    synthetic: bool = False
    provider: str
    provider_definitions: str
    history: list[PastMatch]
    similar_current: SimilarBlock | None = None
    similar_h2h: SimilarBlock | None = None
    references: list[Reference] = Field(default_factory=list)
    bridges: list[Bridge] = Field(default_factory=list)
    quotes: list[Quote] = Field(default_factory=list)
    # Historical league baseline, recorded before decision. Not inferred from this match.
    league_sot_per_team: float = Field(gt=0)
    league_conversion: float = Field(gt=0, lt=1)
    baseline_available_at: datetime
    baseline_source_ref: str

    @model_validator(mode='after')
    def temporal_and_identity(self):
        quarters(self.raw_home_ah); quarters(self.ou_line)
        if self.home_id == self.away_id: raise ValueError('Equipos idénticos')
        times = [self.decision_at,self.kickoff,self.baseline_available_at]
        records = self.history + self.references + self.bridges
        for r in records: times += [r.kickoff,r.available_at]
        for b in (self.similar_current,self.similar_h2h):
            if b: times += [b.available_at]
        times += [q.available_at for q in self.quotes]
        if any(t.tzinfo is None or t.utcoffset() is None for t in times):
            raise ValueError('Fechas sin zona horaria')
        if self.decision_at >= self.kickoff: raise ValueError('Solo prepartido FT90')
        if self.baseline_available_at > self.decision_at: raise ValueError('Baseline futuro')
        for r in records:
            if not r.kickoff < r.available_at <= self.decision_at:
                raise ValueError('Historia futura/no disponible al decidir')
        for b in (self.similar_current,self.similar_h2h):
            if b and b.available_at > self.decision_at: raise ValueError('Bloque futuro')
        for q in self.quotes:
            if q.available_at > self.decision_at: raise ValueError('Cuota futura')
            if q.market in ('OVER','UNDER') and q.line_q < 0: raise ValueError('Total negativo')
        ids = [(r.team_id,r.match_id) for r in self.history]
        if len(set(ids)) != len(ids): raise ValueError('Historia duplicada para el mismo equipo')
        if any(r.team_id not in (self.home_id,self.away_id) or r.team_id == r.opponent_id for r in self.history):
            raise ValueError('Equipo histórico no válido')
        if any(r.match_id == self.event_id for r in records): raise ValueError('Resultado del propio evento en entradas')
        # Two perspectives of the same event must agree on score and source line.
        grouped = {}
        for r in self.history:
            if r.match_id in grouped:
                a = grouped[r.match_id]
                if not (a.team_id == r.opponent_id and a.opponent_id == r.team_id and
                        a.gf == r.ga and a.ga == r.gf and a.venue != r.venue and
                        a.raw_home_ah == r.raw_home_ah and a.ou_line == r.ou_line):
                    raise ValueError('Perspectivas incompatibles del mismo evento')
            grouped[r.match_id] = r
        refs = {}
        for r in self.references + self.bridges:
            if r.match_id in refs and refs[r.match_id] != r.total_goals:
                raise ValueError('Referencia repetida con marcador diferente')
            if r.match_id in grouped and grouped[r.match_id].gf + grouped[r.match_id].ga != r.total_goals:
                raise ValueError('Referencia e historial no concuerdan')
            refs[r.match_id] = r.total_goals
        return self


class Config(Strict):
    version: str = '6.2.0-binary'
    active_days: int = Field(default=21, ge=1)
    history_days: int = Field(default=90, ge=1)
    active_n: int = Field(default=5, ge=1)
    active_weight: float = Field(default=.65, ge=0, le=1)
    role_tolerance_q: int = Field(default=3, ge=0)
    home_advantage_q: float = Field(default=2, ge=0)
    conversion_prior_shots: float = Field(default=20, gt=0)
    min_active_n: int = Field(default=3, ge=1)
    min_similar_n: int = Field(default=2, ge=1)
    sot_advantage: float = 1.5
    ap_advantage: float = 10
    ap_total: float = 100
    shots_total: float = 22
    conversion_high: float = .35
    bubble_conversion: float = .60
    bubble_max_sot: float = 4
    exclude_red_cards: bool = True


class Issue(Strict):
    severity: Literal['info','warning','blocking']
    category: Literal['data','leakage','settlement','statistics','logic','implementation']
    statement: str
    evidence_refs: list[str] = Field(min_length=1)
    suggested_test: str


class Hypothesis(Strict):
    statement: str
    falsification_test: str
    required_data: list[str]


class Review(Strict):
    packet_sha256: str
    verdict: Literal['insufficient_data','needs_revision','ready_for_validation']
    summary: str
    issues: list[Issue]
    hypotheses: list[Hypothesis]
    limitations: list[str]
