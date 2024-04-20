import numpy as np
import pandas as pd
from pypfopt import EfficientFrontier
from pypfopt import expected_returns
from pypfopt import risk_models


def get_capm_weights(df: pd.DataFrame):
    """
    Runs capm model to get portfolio weights
    :param returns_df: Dataframe of ticker log returns (wide form)
    :returns Pandas dataframe with weight of each ticker
    """
    simple_returns_df = np.exp(df) - 1
    mu = expected_returns.mean_historical_return(simple_returns_df, returns_data=True)
    S = risk_models.sample_cov(simple_returns_df, returns_data=True)
    ef = EfficientFrontier(mu, S)
    optimal_weights_portfolio_max_sharpe = ef.max_sharpe()
    optimal_weights_portfolio_max_sharpe = np.array(list(optimal_weights_portfolio_max_sharpe.values()))
    weights_df = round(pd.DataFrame(optimal_weights_portfolio_max_sharpe, index=simple_returns_df.columns), 2)
    weights_df.reset_index(inplace=True)
    weights_df.columns = ['ticker', 'weight']
    return weights_df
