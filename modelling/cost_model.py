"""Reusable loader for the fitted fatigue cost model artifact. This is the
callable interface Phase 4/5's optimiser is meant to use -- it never needs to
know about statsmodels, formulas, or the pickle format directly.
"""
import pickle

from modelling.train import ARTIFACT_PATH


class FatigueCostModel:
    def __init__(self, artifact_path=ARTIFACT_PATH):
        with open(artifact_path, 'rb') as f:
            artifact = pickle.load(f)
        self._model = artifact['model']
        self.fatigue_weights = artifact['fatigue_weights']
        self.control_columns = artifact['control_columns']
        self.fatigue_columns = artifact['fatigue_columns']
        self.beats_baseline = artifact['beats_baseline']

    def predict(self, features_df):
        """Predicted point margin for each row of a features DataFrame with
        the same columns the model was trained on.
        """
        return self._model.predict(features_df)


def load_cost_model(artifact_path=ARTIFACT_PATH):
    return FatigueCostModel(artifact_path)
