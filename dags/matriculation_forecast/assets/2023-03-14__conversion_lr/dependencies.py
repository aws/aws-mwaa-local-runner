import numpy as np
from sklearn.linear_model import RidgeCV

class LogitRegression(RidgeCV):

    def fit(self, x, p, weights=None):
        y = np.log(p / (1 - p)).replace([np.inf, -np.inf], 0)
        if not isinstance(weights, type(None)):
            y = y/weights
        return super().fit(x, y)

    def predict(self, x):
        y = super().predict(x)
        return 1 / (np.exp(-y) + 1)