# Trial model evaluation (task 4.5)

Generated 2026-10-06 20:36 UTC by `make evaluate-model`.

## Protocol

- Each trial is scored with its latest prediction on or before day 7 of the
  trial. A trial with no prediction by then (no activity yet) is scored at the running
  base rate of trials resolved so far, for both models. The label arrives at trial end
  (about day 14 to 17), so the scoring time does not depend on the label.
- Prequential: a trial is scored when its label arrives, then the logistic regression
  learns from the same day-7 vector it was scored on.
- Learner: StandardScaler and LogisticRegression; log1p count features, days into trial
  as a fraction of 14, Adam (learning rate 0.003), L2 0.001.
- Warm start: every trial resolved before the cutoff (2026-09-04); the table uses its last 30%, after the model has warmed up.
- Holdout: the 95 trials resolved after the cutoff, replayed from prepared data with the same code (a 5-minute demo take resolves about
  one trial, too few to evaluate).
- Constant: always predicts the slice's observed conversion rate. It knows that rate in
  advance, so it is a strict reference for log loss.

## Results

| Slice | Model | n | Observed | Mean prediction | Log loss | AUC | Top-decile lift |
|---|---|---:|---:|---:|---:|---:|---:|
| warm start, last 30% | logistic regression | 851 | 0.363 | 0.353 | 0.633 | 0.587 | 1.68 |
| warm start, last 30% | points baseline | 851 | 0.363 | 0.554 | 0.768 | 0.586 | 1.49 |
| warm start, last 30% | constant (observed rate) | 851 | 0.363 | 0.363 | 0.655 | 0.500 | 1.00 |
| warm start, last 30% | (scored at base rate: 0 of 851) | | | | | | |
| holdout after cutoff | logistic regression | 95 | 0.400 | 0.383 | 0.672 | 0.540 | 1.11 |
| holdout after cutoff | points baseline | 95 | 0.400 | 0.552 | 0.775 | 0.537 | 1.11 |
| holdout after cutoff | constant (observed rate) | 95 | 0.400 | 0.400 | 0.673 | 0.500 | 1.00 |
| holdout after cutoff | (scored at base rate: 0 of 95) | | | | | | |

Holdout AUC, bootstrap 95% CI (2,000 resamples of trials, both models on the same draw):

| | AUC | 95% CI |
|---|---:|---:|
| logistic regression | 0.540 | 0.416 to 0.664 |
| points baseline | 0.537 | 0.424 to 0.646 |
| difference | +0.003 | -0.099 to +0.110 |

## Other slices (logistic regression)

| Slice | n | Observed | Mean prediction | Log loss | Constant log loss | AUC | Top-decile lift |
|---|---:|---:|---:|---:|---:|---:|---:|
| warm start, all | 2835 | 0.349 | 0.368 | 0.641 | 0.647 | 0.564 | 1.45 |
| warm start, before 2026-04-08 | 2309 | 0.347 | 0.373 | 0.642 | 0.646 | 0.558 | 1.43 |
| warm start, from 2026-04-08 | 526 | 0.356 | 0.344 | 0.633 | 0.651 | 0.576 | 1.57 |

## What the running system produced

From `ops.trial_outcomes` (baseline warm start, and demo takes since the last reset).

| Slice | n | Observed | Mean prediction | Log loss | Constant log loss | AUC | Top-decile lift |
|---|---:|---:|---:|---:|---:|---:|---:|
| logreg, warm | 2835 | 0.349 | 0.368 | 0.641 | 0.647 | 0.564 | 1.44 |
| logreg, live | 1 | 0.000 | 0.330 | 0.401 | 0.000 | n/a | n/a |
| points, warm | 2835 | 0.349 | 0.571 | 0.805 | 0.647 | 0.557 | 1.25 |
| points, live | 1 | 0.000 | 0.625 | 0.981 | 0.000 | n/a | n/a |

## Warm start updates the weights

After the warm start the logistic regression has 15 weights, L1 norm 0.86 (from 0). Largest:

| Feature | Weight |
|---|---:|
| prior_personal_device | +0.439 |
| gated_attempts | +0.078 |
| is_nonprofit | -0.072 |
| max_active_users | +0.060 |
| distinct_os | +0.060 |
| max_tagged_resources | +0.039 |
| devices_registered | +0.027 |
| distinct_features | -0.020 |

## Rolling AUC

![Rolling AUC](../img/model_rolling_auc.svg)

Rolling AUC over the last 100 resolved trials. Dashed lines mark 2026-04-08 (new
signups move to the current price version) and the cutoff (2026-09-04).

## Why day-7 scoring

The first protocol scored each trial at its last prediction before the label. Converted
trials were scored slightly later on average (day 13.4 vs 13.2), so the model learned
label timing through days into trial: AUC 0.707 on the late warm start fell to 0.604
with that feature held constant. Scoring at a fixed day removes the effect.
