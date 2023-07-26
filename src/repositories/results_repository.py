from pandas import DataFrame

from ..db.engine import engine


def save_results(dataframe: DataFrame) -> None:
    dataframe.to_sql('results', engine, if_exists='replace', index=False, method='multi')
