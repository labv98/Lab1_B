
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: Aquí se encuentran las funciones del lab1 de MYST con POO                                  -- #
# -- script: functions.py : python script with general functions                                         -- #
# -- author: labv98                                                                      -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: https://github.com/labv98/Lab1_B.git                                                   -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

import glob
import pandas as pd
import numpy as np


def read_df(path):
    filenames = glob.glob(path + "/*.csv")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename, skiprows=2))
    # Concatenar la info de todos los archivos a uno solo
    df = pd.concat(dfs, ignore_index=True)
    df = df.loc[:, ['Ticker', 'Peso (%)']]
    df = df.astype({'Ticker': str,
                    'Peso (%)': float})
    # Reemplazo de información
    df.replace(to_replace="GFREGIOO", value="RA")
    df.replace(to_replace="MEXCHEM", value="ORBIA")
    df.replace(to_replace="LIVEPOLC.1", value="LIVEPOLC-1")
    df.replace(to_replace="SITESB.1", value="SITESB-1")
    df['Ticker'] = [i.replace('*', '')+".MX" for i in df['Ticker']]
    # Quitar los NAN'S
    df.dropna(subset=['Peso (%)'], inplace=True)
    # Pasar los pesos a decimal
    df['Peso (%)'] = df['Peso (%)']/100
    return df
