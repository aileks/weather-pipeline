# Anomaly Detection

`weather_anomalies` answers one question per row: *was this hour's value, at this city, unusually far from what this hour of day usually sees here?* It is a deliberately simple, explainable heuristic. It is not a statistically robust detector, it does not model weather physics, and its weakest variable (precipitation) is kept only with an explicit warning label. This document owns the full specification, including the numeric constants (the 30-day baseline window, the 14-comparable guard, the z threshold of 3.0); other documents summarize and link here. The mart's mechanical details (grain, tests) are in [Transformation](transformation.md#weather_anomalies); the decision record is in [Design Decisions](design-decisions.md).

## The heuristic

For every observation of a covered variable at hour *t*:

- **Comparables:** observations of the *same variable*, at the *same location*, at the *same hour of day (UTC)*, in the trailing 30 days before *t* (strictly earlier: the window is `[t - 30 days, t)`). Roughly 30 comparable values; never including the observation being scored.
- **Baseline:** mean and sample standard deviation (`stddev_samp`) of the comparables.
- **Score:** `z = (observed - baseline_mean) / baseline_std` (no row emitted if the standard deviation is 0).
- **Guard:** a score is emitted only with at least **14 comparables**. A new city or a young partition window produces silence, not noise.
- **Anomaly:** `abs(z) >= 3.0`.

### Why hour-of-day matching

Temperature, pressure, and wind all have strong diurnal cycles. Scoring 14:00 against a baseline pooled over all hours of the day would let "afternoon is warmer than the 24-hour average" masquerade as an anomaly, and real 03:00 anomalies would drown in the inflated pooled variance. Comparing like against like (same hour of day) removes the cycle from the baseline by construction rather than by modeling it. UTC is the canonical basis everywhere in this project; hour-of-day matching therefore means *UTC* hour of day, which is stable and DST-proof even if it is not the city's local hour.

### Worked example

Reykjavík, 2026-08-15 14:00 UTC, temperature 16.1 degrees C. The 30 comparables are Reykjavík temperatures at 14:00 UTC on each of the previous 30 days. Suppose their mean is 10.2 and sample standard deviation is 1.8:

```
z = (16.1 - 10.2) / 1.8 = 3.28    ->  anomaly (abs(z) >= 3)
```

The emitted row reads: location `reykjavik`, hour `2026-08-15 14:00 UTC`, variable `temperature_c`, observed `16.1`, baseline mean `10.2`, baseline std `1.8`, `comparable_obs_count 30`, `z_score 3.28`. Every number in it is a sentence a human can repeat: "that hour was 3.3 standard deviations above what 14:00 usually felt like there over the past month."

## Variables covered

| Variable | Suitability | Notes |
|---|---|---|
| `temperature_c` | Good | Diurnal-matched baseline makes z-scores meaningful |
| `surface_pressure_hpa` | Good | Hour-of-day matching handles the semi-diurnal pressure tide |
| `wind_speed_kmh` | Acceptable | Right-skewed; z-scores over-flag the top tail relative to a fitted distribution, acceptable for a heuristic |
| `precipitation_mm` | **Weak, kept deliberately** | See below |

**Precipitation warning.** Hourly precipitation is zero-heavy and strongly skewed: at most hours in most cities it is exactly 0. A mean/std baseline over such a distribution is a poor model, and a large z-score mostly encodes "it rained, and it usually does not", not "it rained extraordinarily". The variable is kept because the mart's purpose is practicing the *pattern* (unpivot, score, filter, publish), and the emission `precipitation_mm` anomalies remain directionally interesting. But no analytical conclusion should lean on precipitation z-scores, and the model documentation says so in place. Better precipitation detectors (wet-day-conditioned baselines, percentile/EGPD-style extreme-value approaches) are deliberate future improvements.

## Implementation shape

Realized as dbt SQL in the marts layer, using the `z_score` macro registered in [Transformation](transformation.md#project-layout). The shape (abridged, not the literal model):

```sql
with hourly as (
    -- unpivot covered measures into (location, hour, variable, value) rows
),
scored as (
    select
        h.location_id, h.hour_ts_utc, h.variable, h.value as observed_value,
        avg(c.value)     as baseline_mean,
        stddev_samp(c.value) as baseline_std,
        count(*)         as comparable_obs_count
    from hourly h
    join hourly c
      on  c.location_id = h.location_id
      and c.variable    = h.variable
      and extract(hour from c.hour_ts_utc) = extract(hour from h.hour_ts_utc)
      and c.hour_ts_utc >= h.hour_ts_utc - interval 30 day
      and c.hour_ts_utc <  h.hour_ts_utc
    group by 1, 2, 3, 4
)
select
    location_id, hour_ts_utc, variable, observed_value,
    baseline_mean, baseline_std, comparable_obs_count,
    (observed_value - baseline_mean) / nullif(baseline_std, 0) as z_score
from scored
where comparable_obs_count >= 14
  and abs((observed_value - baseline_mean) / nullif(baseline_std, 0)) >= 3
```

Properties worth stating outright:

- **Deterministic:** no randomness, no tie-breaking ambiguity; rebuilding the mart from the same fact yields identical rows. (This is why the output carries no `detected_at` column: nothing about detection is time-of-run dependent.)
- **Recomputes from converged truth:** the mart rebuilds fully on each unpartitioned-group run ([Orchestration](orchestration.md#two-dbt-asset-groups)). When reconciliation revises a provisional value ([Ingestion & Storage](ingestion-storage.md#provisional-values-why-reconciliation-exists)), the score is recomputed against revised history. A flag can therefore appear, then disappear once the source revises: the mart always reflects current truth, and the landing zone preserves what was previously believed.
- **Baseline uses history, not the future:** the strict `[t-30d, t)` window means scoring hour *t* never consults hours after *t*. Backfilled and live-scored days are interchangeable.

## Why this and not the alternatives

- **Fixed thresholds** ("temp > 40 C is an anomaly") are trivially explainable but ignore location and season: 40 C is a normal Cairo afternoon and a catastrophe in Reykjavík. A threshold per city per season is a worse-maintained version of what the baseline already computes.
- **Climatology baselines** (same calendar day across years) are meteorologically the right answer but need years of history; this project's window starts 2026-07-01. The trailing 30-day baseline works from day 15 and adapts to seasons by construction.
- **Fitted distributions / extreme-value models** are more robust for skewed variables and are the future answer for precipitation, but cost explainability. The project chose the heuristic whose output a reviewer can verify by hand.

## Interpretation guidance

An anomaly row is a *flag for a human*, not an event verdict. It means "this hour was rare against the last month of comparable hours". Three honest readings:

1. Genuine weather event (heatwave hour, pressure jump ahead of a front).
2. Baseline artifact (an unusually calm month makes ordinary look extreme).
3. Upstream revision artifact (a provisional value scored before reconciliation converged; re-check after the day converges).

The `comparable_obs_count` column exists so a reader can discount scores built on thin history, and `baseline_mean`/`baseline_std` exist so every score is auditable with a calculator.
