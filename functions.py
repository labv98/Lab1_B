
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: Aquí se encuentran las funciones del lab1 de MYST con POO                                  -- #
# -- script: functions.py : python script with general functions                                         -- #
# -- author: labv98                                                                      -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: https://github.com/labv98/Lab1_B.git                                                   -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

# ---- LIBRERÍAS ----

import glob
import pandas as pd
import numpy as np
import yfinance as yf

# ---- TRATAMIENTO DE DATOS ----


class FilesHandler:
    @staticmethod
    def read_df(path):
        """Esta función lee los archivos de la carpeta llamada files
        donde se encuentran los ETF, una vez que los lee los concatena en
        un DataFrame y realiza el tratamiento de datos para quedarnos con lo
        necesario para el análisis."""

        filenames = glob.glob(path + "/*.csv")
        dfs = []
        for filename in filenames:
            dfs.append(pd.read_csv(filename, skiprows=2))
        # Concatenar la info de todos los archivos a uno solo
        df = pd.concat(dfs, ignore_index=True)
        df = df.loc[:, ['Ticker', 'Nombre', 'Peso (%)', 'Precio']]
        # Reemplazo de información
        df['Ticker'] = [i.replace("GFREGIOO", "RA") for i in df['Ticker']]
        df['Ticker'] = [i.replace("MEXCHEM", "ORBIA") for i in df['Ticker']]
        df['Ticker'] = [i.replace("LIVEPOLC.1", "LIVEPOLC-1") for i in df['Ticker']]
        df['Ticker'] = [i.replace("SITESB.1", "SITESB-1") for i in df['Ticker']]
        df['Ticker'] = [i.replace('*', '') + ".MX" for i in df['Ticker']]
        # Quitar los NAN'S
        df.dropna(subset=['Peso (%)'], inplace=True)
        # Pasar los pesos a decimal
        df['Peso (%)'] = df['Peso (%)'] / 100
        # Quitar los activos que no necesitamos
        idx = df.set_index("Ticker", inplace=True)
        df.drop(['KOFL.MX', 'KOFUBL.MX', 'USD.MX', 'BSMXB.MX', 'NMKA.MX', 'MXN.MX'], inplace=True)

        return df


class TickersHandler:
    @staticmethod
    def tickers():
        """Esta función descarga los datos por parte de yahoo finance y los
        organiza respecto al precio de cierre de los activos denominados como
        tickers y guardados en esa variable."""

        tickers = df.index.tolist()
        tickers = np.unique(tickers).tolist()
        data_down = yf.download(tickers, start="2018-01-31", actions=False,
                                group_by="close", interval='1d', auto_adjust=False,
                                prepost=False, threads=True)
        data_close = pd.DataFrame({i: data_down[i]['Close'] for i in tickers})
        return tickers, data_down, data_close

    @staticmethod
    def f_data_fin(path, k, c):
        """Esta función lee el primer archivo de todos los contenidos en la carpeta de
        files correspondiente a NAFTRAC_20180131, hace su tratamiento de datos
        correspondiente y nos regresa un DataFrame que contiene los activos, pesos, títulos,
        precios, comisiones y la postura."""

        file = pd.read_csv(path, skiprows=2)
        data_fin = file.copy().sort_values('Ticker')[['Ticker', 'Peso (%)']]
        # Reemplazo de información
        data_fin['Ticker'] = [i.replace("GFREGIOO", "RA") for i in data_fin['Ticker']]
        data_fin['Ticker'] = [i.replace("MEXCHEM", "ORBIA") for i in data_fin['Ticker']]
        data_fin['Ticker'] = [i.replace("LIVEPOLC.1", "LIVEPOLC-1") for i in data_fin['Ticker']]
        data_fin['Ticker'] = [i.replace("SITESB.1", "SITESB-1") for i in data_fin['Ticker']]
        data_fin['Ticker'] = [i.replace('*', '') + ".MX" for i in data_fin['Ticker']]
        # Quitar los NAN'S
        data_fin.dropna(subset=['Peso (%)'], inplace=True)
        # Pasar los pesos a decimal
        data_fin['Peso (%)'] = data_fin['Peso (%)'] / 100
        # Quitar los activos que no necesitamos
        data_fin.set_index("Ticker", inplace=True)
        data_fin.drop(['KOFL.MX', 'BSMXB.MX', 'MXN.MX'], inplace=True)

        data_fin['Precios'] = data_close[data_fin.index].iloc[0, :]
        data_fin['Titulos'] = np.floor((k * data_fin['Peso (%)']) /
                                       (data_fin['Precios'] + (data_fin['Precios'] * c)))

        data_fin['Capital'] = np.round(data_fin['Titulos'] *
                                       (data_fin['Precios'] +
                                        (data_fin['Precios'] * c)), 2)
        data_fin['Postura'] = np.round(data_fin['Precios'] * data_fin['Titulos'], 2)
        data_fin['Comisiones'] = np.round(data_fin['Precios'] * c * data_fin['Titulos'], 2)

        return data_fin


class PassiveInvestment:
    def __init__(self, k, c, data_fin, data_close) -> None:
        self.data_fin = data_fin
        self.data_close = data_close
        self.inv_pasiva(k, c, data_fin, data_close)

    def inv_pasiva(self, k, c, data_fin, data_close):
        """Función que retorna un DataFrame con la información completa de una
        inversión pasiva desde inicio de la fecha en los archivos hasta el final
        del periodo que es en octubre 2021."""

        # Cash
        cash = (1 - data_fin['Peso (%)'].sum()) * k
        # Comisiones
        comision_sum = data_fin['Comisiones'].sum()
        ind_pas = []
        for i in range(1, len(data_close.index)):
            if data_close.index[i - 1].month != data_close.index[i].month:
                ind_pas.append(data_close.index[i - 1])
        df_pasiva = pd.DataFrame(columns=['Fecha', 'Capital', 'Rendimiento', 'Rendimiento Acumulado'])
        df_pasiva['Fecha'] = ind_pas
        capitales = (data_close.T[df_pasiva.Fecha].T[data_fin.index]) * data_fin['Titulos'] + data_fin['Comisiones']
        df_pasiva['Capital'] = list(capitales.sum(axis=1) + cash)
        pd.options.display.float_format = '{:.4f}'.format
        df_pasiva['Rendimiento'] = df_pasiva['Capital'].pct_change().fillna(0)
        df_pasiva['Rendimiento Acumulado'] = (df_pasiva['Capital'] - df_pasiva.loc[0, 'Capital']) / df_pasiva.loc[
            0, 'Capital']
        return df_pasiva

    @staticmethod
    def ant_pan():
        """Función que muestra el DataFrame de la inversión pasiva antes de pandemia."""

        df_pasiva_ap = df_pasiva.loc[0:24]
        return df_pasiva_ap

    @staticmethod
    def dur_pan():
        """Función que muestra el DataFrame de la inversión pasiva durante pandemia."""

        df_pasiva_dp = df_pasiva.loc[24:]
        df_pasiva_dp['Rendimiento Acumulado'] = df_pasiva_dp['Rendimiento'].cumsum()
        return df_pasiva_dp