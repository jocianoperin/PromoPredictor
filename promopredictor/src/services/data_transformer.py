import pandas as pd
import holidays

def add_temporal_features(df):
    """
    Adiciona features temporais e sazonais ao DataFrame.
    """
    df['DATA'] = pd.to_datetime(df['DATA'])
    df['dia_da_semana'] = df['DATA'].dt.dayofweek
    df['mes'] = df['DATA'].dt.month
    df['dia'] = df['DATA'].dt.day
    df['ano'] = df['DATA'].dt.year
    br_holidays = holidays.Brazil()
    df['feriado'] = df['DATA'].apply(lambda x: 1 if x in br_holidays else 0)
    return df

def add_lag_features(df, group_col, target_col, lags):
    """
    Adiciona features de lag ao DataFrame.
    """
    for lag in lags:
        df[f'{target_col}_lag{lag}'] = df.groupby(group_col)[target_col].shift(lag)
    return df

def add_rolling_average(df, group_col, target_col, windows):
    """
    Adiciona médias móveis ao DataFrame.
    """
    for window in windows:
        df[f'{target_col}_{window}d_avg'] = df.groupby(group_col)[target_col].transform(
            lambda x: x.shift(1).rolling(window, min_periods=1).mean())
    return df
