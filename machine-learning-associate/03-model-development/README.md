# 03 — Model Development (31%)

The second-heaviest section: the Spark ML object model (estimator vs
transformer — naming a class's category is a literal question format),
pipelines as self-enforcing leakage protection, CrossValidator vs
TrainValidationSplit economics, class-imbalance levers, Hyperopt's
`fmin`/space/`SparkTrials` machinery, and metric selection by cost of error.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_pipelines_and_validation.py`](01_pipelines_and_validation.py) | A full prep+model `Pipeline` fit and applied, estimator/transformer taxonomy, `CrossValidator` (k×grid economics) vs `TrainValidationSplit`, the imbalance table with `weightCol` live |
| [`02_tuning_and_metrics.py`](02_tuning_and_metrics.py) | Hyperopt `fmin` + TPE run live (negate to maximize!), `hp.*` space vocabulary, `SparkTrials` vs `Trials` (the classic swap), classification and regression metric tables, the bias-variance diagnosis grid |

## Setup

Run section 01's `00_setup.py` first; notebook 02's Hyperopt cell wants an
ML Runtime cluster and degrades gracefully elsewhere.

## How to study

1. Drill the taxonomy until instant: estimator `.fit()` → transformer
   `.transform()`; `Pipeline` is an estimator, `PipelineModel` a
   transformer.
2. Metrics questions are cost questions: name which error is expensive, the
   metric follows (FP → precision, FN → recall, both+imbalance → F1,
   probabilities → log loss; big misses → RMSE, outlier-robust → MAE).
3. Then drill this section on [TestHub](https://testlogichub.com) — 31% ≈ 14
   of 45 questions.
