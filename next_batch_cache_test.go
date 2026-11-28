package zmux

import (
	"bytes"
	"errors"
	"math"

	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func testCarrierCache(t testing.TB, opts CarrierCacheOptions) *CarrierCache {
	t.Helper()
	cache, err := NewCarrierCache(opts)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestNextBatchHCarrierCache(t *testing.T) {
	start := time.Unix(1800000000, 0)
	clk := timeshim.NewFake(start)
	backoff := Backoff{Min: 30 * time.Second, Max: 30 * time.Minute}
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clk), Backoff: backoff})
	report := func(outcome CacheOutcome, samples ...CarrierObservation) {
		t.Helper()
		var sample CarrierObservation
		if len(samples) != 0 {
			sample = samples[0]
		}
		if err := cache.update(CarrierCacheKey{Key: "key", Kind: CarrierRaw, Variant: "variant"}, outcome, sample); err != nil {
			t.Fatal(err)
		}
	}
	score := func() CarrierScore { return cache.Score("key", CarrierRaw, "variant") }
	closeTo := func(got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-10 {
			t.Fatalf("score %g, want %g", got, want)
		}
	}
	closeTo(score().Score, .125)
	report(CacheUsable)
	report(CachePerformance, CarrierObservation{At: start, RTT: 25 * time.Millisecond, Rate: 2_500_000})
	closeTo(score().Score, (2.0/3)*.25*2)
	clk.Advance(59 * time.Second)
	report(CachePerformance, CarrierObservation{At: start, RTT: time.Millisecond})
	if score().Score < .3 {
		t.Fatal("sample expired before its window")
	}
	clk.Advance(time.Second)
	closeTo(score().Score, ((1+decayAge(time.Minute, 6*time.Hour))/(2+decayAge(time.Minute, 6*time.Hour)))*.25)
	for range 3 {
		report(CacheShortDrop)
	}
	until := score().CoolingUntil
	if until.IsZero() {
		t.Fatal("short drops did not cool the candidate")
	}
	if cache.Score("key", CarrierRaw, "").Attempts != 0 {
		t.Fatal("variants share evidence")
	}
	snapshot, err := cache.Export()
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	restarted := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clk), Backoff: backoff})
	if err := restarted.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	got := restarted.Score("key", CarrierRaw, "variant")
	if got.Attempts != 1 || got.CoolingUntil.Sub(until).Abs() >= time.Millisecond {
		t.Fatalf("restored %+v, want deadline %v", got, until)
	}
	if err := restarted.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	if actual := restarted.Score("key", CarrierRaw, "variant"); actual != got {
		t.Fatalf("replay changed score from %+v to %+v", got, actual)
	}
	before, _ := restarted.Export()
	for _, entry := range []struct {
		want  error
		input string
	}{{input: `{"version":1}`, want: ErrCarrierCacheVersion}, {input: `{"version":2`, want: ErrCarrierCacheFormat}, {input: `{"version":2}`, want: ErrCarrierCacheValue}} {
		if err := restarted.Import([]byte(entry.input)); !errors.Is(err, entry.want) {
			t.Fatalf("Import(%q) = %v, want %v", entry.input, err, entry.want)
		}
	}
	after, _ := restarted.Export()
	if !bytes.Equal(before, after) {
		t.Fatal("invalid import mutated cache")
	}
}

func TestNextBatchHCachePerformanceAgeAcrossClockRollback(t *testing.T) {
	start := time.Unix(1800000000, 0)
	clock := timeshim.NewFake(start)
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock)})
	if err := cache.update(CarrierCacheKey{Key: "test", Kind: CarrierRaw}, CachePerformance, CarrierObservation{At: start, RTT: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(39 * time.Second)
	snapshot, err := cache.Export()
	if err != nil {
		t.Fatal(err)
	}
	clock = timeshim.NewFake(start.Add(-time.Hour))
	restart := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock)})
	if err := restart.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Second)
	score := func() float64 { return restart.Score("test", CarrierRaw, "").Score }
	if score() <= .125 {
		t.Fatal("fresh evidence discarded on restart")
	}
	clock.Advance(time.Second)
	if got := score(); got != .125 {
		t.Fatalf("expired evidence changed the score to %v", got)
	}
}

func TestNextBatchHProtocolEstimatorWindowSurvivesExport(t *testing.T) {
	clock := timeshim.NewFake(time.Unix(1800000000, 0))
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock)})
	observation := CarrierObservation{At: clock.Now(), Rate: 2_500_000, ValidFor: 10 * time.Second}
	if err := cache.update(CarrierCacheKey{Key: "network", Kind: CarrierRaw}, CachePerformance, observation); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	snapshot, err := cache.Export()
	if err != nil {
		t.Fatal(err)
	}
	restart := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock)})
	if err := restart.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	if score := restart.Score("network", CarrierRaw, "").Score; score <= .125 {
		t.Fatalf("fresh rate score %g", score)
	}
	clock.Advance(time.Second)
	if score := restart.Score("network", CarrierRaw, "").Score; score != .125 {
		t.Fatalf("expired rate score %g", score)
	}
}

func TestCarrierScoreOrderBuckets(t *testing.T) {
	last := 1.0 + 0.4e-9
	next := 1.0 + 0.6e-9
	if carrierScoreOrder(1) != carrierScoreOrder(last) || carrierScoreOrder(last) >= carrierScoreOrder(next) {
		t.Fatal("incorrect D160 score bucket boundary")
	}
	if carrierScoreOrder(math.Copysign(0, -1)) != carrierScoreOrder(0) {
		t.Fatal("zero representations sort differently")
	}
}

func TestCarrierReportSourceAppliesEachSequenceOnce(t *testing.T) {
	clock := timeshim.NewFake(time.Unix(1800000000, 0))
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock)})
	source, err := cache.RegisterReportSource("key", CarrierRaw, "variant")
	if err != nil {
		t.Fatal(err)
	}
	copyOfSource := source
	if applied, err := source.Report(5, CacheFailed, CarrierObservation{At: clock.Now().Add(time.Second)}); applied || !errors.Is(err, ErrCarrierCacheValue) {
		t.Fatalf("invalid report = %v, %v", applied, err)
	}
	if applied, err := source.Report(5, CacheFailed, CarrierObservation{}); !applied || err != nil {
		t.Fatalf("first report = %v, %v", applied, err)
	}
	if applied, err := copyOfSource.Report(5, CacheFailed, CarrierObservation{}); applied || err != nil {
		t.Fatalf("replayed report = %v, %v", applied, err)
	}
	if got := cache.Score("key", CarrierRaw, "variant"); got.Attempts != 1 || got.CooldownLevel != 1 {
		t.Fatalf("score after replay: %+v", got)
	}
}

func TestCarrierCacheOptionsAreValidatedAndFrozen(t *testing.T) {
	weights := map[CarrierKind]float64{CarrierQUIC: 2}
	options := CarrierCacheOptions{KindPriors: weights}
	cache := testCarrierCache(t, options)
	weights[CarrierQUIC] = 3
	options.Backoff = Backoff{Min: -1}
	if got := cache.Score("route", CarrierQUIC, "").Score; got != 0.25 {
		t.Fatalf("score with frozen prior = %g, want .25", got)
	}
	if _, err := NewCarrierCache(options); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid configuration at construction: %v", err)
	}
	var defaults CarrierCache
	if got := defaults.Score("route", CarrierQUIC, "").Score; got != 0.125 {
		t.Fatalf("zero cache score = %g, want .125", got)
	}
}
