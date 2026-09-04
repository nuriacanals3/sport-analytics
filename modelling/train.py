"""Fits the fatigue cost model: a linear regression of point margin on
fatigue/travel features, controlling for team strength and home-court.

Temporal split (never mix future into past, per the travel-logistics plan):
train on the four older seasons, evaluate on the held-out most recent one.

statsmodels (not scikit-learn) is used deliberately: the plan's goal here is
"estimating effects, not winning a prediction contest", and statsmodels'
OLS gives coefficients with standard errors and p-values out of the box,
so you can see which fatigue effects are statistically real, not just guess.

Simplification: model strictly additive and linear, each feature's effect is
linear and independent. No combination of attributes, ex: "back-to-backs hurt 
more when you're also on the road"
"""
import pickle

import numpy as np
import statsmodels.formula.api as smf

from modelling.features import (
    CONTROL_COLUMNS,
    FATIGUE_COLUMNS,
    TARGET_COLUMN,
    load_features,
)

TRAIN_SEASONS = ['2018-19', '2021-22', '2022-23', '2023-24']
HELD_OUT_SEASON = '2024-25'

# R style (~ = predict this from that)
BASELINE_FORMULA = f'{TARGET_COLUMN} ~ is_home'
MODEL_FORMULA = f'{TARGET_COLUMN} ~ ' + ' + '.join(CONTROL_COLUMNS + FATIGUE_COLUMNS)

ARTIFACT_PATH = 'modelling/artifacts/fatigue_cost_model.pkl'


def rmse(actual, predicted):
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def out_of_sample_r2(actual, predicted):
    """R^2 computed on held-out data (not the model's own .rsquared, which is
    an in-sample training-fit statistic and would be misleading here).
    """
    ss_res = np.sum((actual - predicted) ** 2)
    ss_tot = np.sum((actual - actual.mean()) ** 2)
    return float(1 - ss_res / ss_tot)


def evaluate(fitted_model, test_df):
    predicted = fitted_model.predict(test_df)
    actual = test_df[TARGET_COLUMN]
    return {'rmse': rmse(actual, predicted), 'r2': out_of_sample_r2(actual, predicted)}

# ols = Ordinary Least Squares linear regression
# predicted_margin = intercept
#                  + β₁·is_home
#                  + β₂·self_net_rating
#                  + β₃·opponent_net_rating
#                  + β₄·rest_days_diff
#                  + β₅·travel_miles_diff
#                  + β₆·timezones_shift_diff
#                  + β₇·back_to_back_diff
# Least squares = find the set of coefficients (β₁..β₇) + intercept 
# that makes the total squared prediction error smallest: minimize Σ(actual_margin − predicted_margin)²
def main():
    df = load_features()
    train_df = df[df['season'].isin(TRAIN_SEASONS)]
    test_df = df[df['season'] == HELD_OUT_SEASON]
    print(f"Train rows: {len(train_df)} ({TRAIN_SEASONS})")
    print(f"Held-out rows (test): {len(test_df)} ({HELD_OUT_SEASON})")

    baseline = smf.ols(BASELINE_FORMULA, data=train_df).fit()
    model = smf.ols(MODEL_FORMULA, data=train_df).fit()

    baseline_metrics = evaluate(baseline, test_df)
    model_metrics = evaluate(model, test_df)

    # To evaluate performance on regression models, compare this metrics between baseline and all features: 
    # - RMSE (Root Mean Squared Error)
    # - R2 (R-squared): how % the model explains of the test season variance
    print("\n=== Baseline (home-court only) for held-out (test) season ===")
    print(f"RMSE: {baseline_metrics['rmse']:.3f}   R2: {baseline_metrics['r2']:.4f}")

    print("\n=== Full model for held-out (test) season ===")
    print(f"RMSE: {model_metrics['rmse']:.3f}   R2: {model_metrics['r2']:.4f}")

    beats_baseline = model_metrics['rmse'] < baseline_metrics['rmse']
    print(f"\nBeats baseline: {beats_baseline}")
    if not beats_baseline:
        print("STOP: the model does not beat the baseline. Per the travel-logistics "
              "plan, the cost feeding the optimiser would be worthless -- reconsider "
              "before proceeding to Phase 4.")

    print("\n=== Full model summary ===")
    print(model.summary())

    fatigue_weights = {col: float(model.params[col]) for col in FATIGUE_COLUMNS}
    print("\n=== Fatigue-feature coefficients (the weights Phase 5 will use) ===")
    for col, coef in fatigue_weights.items():
        print(f"  {col}: {coef:.4f}")

    artifact = {
        'model': model,
        'formula': MODEL_FORMULA,
        'fatigue_weights': fatigue_weights,
        'control_columns': CONTROL_COLUMNS,
        'fatigue_columns': FATIGUE_COLUMNS,
        'train_seasons': TRAIN_SEASONS,
        'held_out_season': HELD_OUT_SEASON,
        'baseline_metrics': baseline_metrics,
        'model_metrics': model_metrics,
        'beats_baseline': beats_baseline,
    }
    with open(ARTIFACT_PATH, 'wb') as f:
        pickle.dump(artifact, f)
    print(f"\nSaved artifact to {ARTIFACT_PATH}")


if __name__ == '__main__':
    main()
