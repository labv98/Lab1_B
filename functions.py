
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

    def __init__(self):
        pass

    @staticmethod
    def read_df():
        """Esta función lee los archivos de la carpeta llamada files
        donde se encuentran los ETF, una vez que los lee los concatena en
        un DataFrame y realiza el tratamiento de datos para quedarnos con lo
        necesario para el análisis."""

        filenames = glob.glob('/Users/alejandrabarraganvazquez/Downloads/MyST/Lab1_B/files' + "/*.csv")
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
        df.set_index("Ticker", inplace=True)
        df.drop(['KOFL.MX', 'KOFUBL.MX', 'USD.MX', 'BSMXB.MX', 'NMKA.MX', 'MXN.MX'], inplace=True)

        return df

    def tickers(self):
        """Esta función descarga los datos por parte de yahoo finance y los
        organiza respecto al precio de cierre de los activos denominados como
        tickers y guardados en esa variable."""
        df = self.read_df()
        tickers = df.index.tolist()
        tickers = np.unique(tickers).tolist()
        data_down = yf.download(tickers, start="2018-01-31", actions=False,
                                group_by="close", interval='1d', auto_adjust=False,
                                progress=False, prepost=False, threads=True)
        data_close = pd.DataFrame({i: data_down[i]['Close'] for i in tickers})
        return tickers, data_down, data_close

    def f_data_fin(self) -> pd.DataFrame:
        """Esta función lee el primer archivo de todos los contenidos en la carpeta de
        files correspondiente a NAFTRAC_20180131, hace su tratamiento de datos
        correspondiente y nos regresa un DataFrame que contiene los activos, pesos, títulos,
        precios, comisiones y la postura."""

        # Datos dados de capital y comisiones
        k = 1000000
        c = 0.00125

        file = pd.read_csv('/Users/alejandrabarraganvazquez/Downloads/MyST/Lab1_B/files/NAFTRAC_20180131.csv',
                           skiprows=2)
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
        tickers, data_down, data_close = self.tickers()
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

# ---- INVERSIÓN PASIVA ----


class PassiveInvestment:

    def __init__(self):
        pass

    @staticmethod
    def f_data_fin():
        return FilesHandler().f_data_fin()

    @staticmethod
    def tickers():
        return FilesHandler().tickers()

    def inv_pasiva(self):
        """Función que retorna un DataFrame con la información completa de una
        inversión pasiva desde inicio de la fecha en los archivos hasta el final
        del periodo que es en octubre 2021."""
        data_fin = self.f_data_fin()
        tickers, data_down, data_close = self.tickers()
        k = 1000000
        # Cash
        cash = (1 - data_fin['Peso (%)'].sum()) * k
        # Comisiones
        # comision_sum = data_fin['Comisiones'].sum()
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

    def ant_pan(self) -> pd.DataFrame:
        """Función que muestra el DataFrame de la inversión pasiva antes de pandemia."""
        df_pasiva = self.inv_pasiva()
        df_pasiva_ap = df_pasiva.loc[0:24]
        return df_pasiva_ap

    def dur_pan(self) -> pd.DataFrame:
        """Función que muestra el DataFrame de la inversión pasiva durante pandemia."""
        df_pasiva = self.inv_pasiva()
        df_pasiva_dp = df_pasiva.loc[24:]
        df_pasiva_dp['Rendimiento Acumulado'] = df_pasiva_dp['Rendimiento'].cumsum()
        return df_pasiva_dp

# ---- PORTAFOLIO ÓPTIMO ----


class Portfolio:

    def __init__(self):
        pass

    @staticmethod
    def tickers():
        return FilesHandler().tickers()

    def portfolios(self):
        """
        Función que regresa el portafolio óptimo para la inversión,
        obteniendo el radio de Sharpe.
        """
        tickers, data_down, data_close = self.tickers()
        # Obtener rendimientos logaritmicos mensuales utilizando propiedad de divison de log
        prices_pre = np.log(data_close).diff()
        # Establecer periodo prepandemia
        prices1 = prices_pre.loc[prices_pre.index <= "2020-02-28", :]
        matriz_cov = prices1.cov() * np.sqrt(12)
        # Definir el numero de portafolios para realizar la frontera eficiente
        p_ret = []
        p_vol = []
        p_weights = []
        p_sharpe = []
        num_assets = len(data_close.columns)
        num_portfolios = 100
        indv_returns = prices1.mean()

        for portfolio in range(num_portfolios):
            weights = np.random.random(num_assets)
            weights = weights / np.sum(weights)
            p_weights.append(weights)
            returns = np.dot(weights, indv_returns * 12) - .0425
            p_ret.append(returns)

            variance = weights.T @ matriz_cov @ weights
            stand_dev = np.sqrt(variance)
            p_vol.append(stand_dev)

            sharpe_r = returns / stand_dev
            p_sharpe.append(sharpe_r)

        data = {'Returns': p_ret, 'Volatility': p_vol, 'Sharpe': p_sharpe}

        for counter, symbol in enumerate(prices1.columns.tolist()):
            data[symbol + ' weight'] = [w[counter] for w in p_weights]
        portfolios = pd.DataFrame(data)

        return portfolios

    @staticmethod
    def f_data_fin():
        return FilesHandler().f_data_fin()

    def f_portfolio1(self):
        portfolios = self.portfolios()
        # Seleccionar el portafolio con mayor sharpe
        max_sharpe = portfolios.iloc[portfolios['Sharpe'].idxmax()]
        max_sharpe = max_sharpe.to_list()
        pesos1 = max_sharpe[10:]
        # Obtener precios despues de pandemia
        tickers, data_down, data_close = self.tickers()
        prices_post1 = data_close.loc[data_close.index >= "2020-02-28", :]
        lista = prices_post1.index.values
        capital = 1000000 - ((1 - self.f_data_fin()['Peso (%)'].sum()) * 1000000)
        #  Cambios porcentuales en pandemia
        prices_post = data_close.pct_change()
        prices_post = prices_post[prices_post.index > "2020-02-28"]
        lista2 = prices_post.index.values
        portfolio_1 = pd.DataFrame()
        data_fin = self.f_data_fin()
        portfolio_1["Ticker"] = data_fin.index
        portfolio_1['Precios'] = (np.array([prices_post1.iloc[0, data_close.columns.to_list().index(i)] for i in data_fin.index]))
        portfolio_1["Peso"] = pesos1
        portfolio_1['Postura'] = np.round(capital * portfolio_1["Peso"], 2)
        portfolio_1['Titulos'] = np.floor((portfolio_1["Postura"] / portfolio_1["Precios"]))
        portfolio_1['Comisiones'] = np.round(portfolio_1['Precios'] * 0.00125 * portfolio_1['Titulos'], 2)
        portfolio_1 = portfolio_1.set_index("Ticker")
        return portfolio_1, prices_post, prices_post1

    def new_port(self):
        data_fin = self.f_data_fin()
        tickers, data_down, data_close = self.tickers()
        portfolio_1, prices_post, prices_post1 = self.f_portfolio1()
        # Obtener tickers
        tickers = data_fin.index
        tickers = tickers.to_list()
        cash = (1 - data_fin['Peso (%)'].sum()) * 1000000
        # Restar comisiones
        cash = cash - portfolio_1["Comisiones"].sum()
        # Crear columna de prices en post pandemia
        prices_post1 = pd.DataFrame(prices_post.iloc[0, :])
        prices_post1.columns = ["Porcentaje"]
        prices_down = prices_post1[prices_post1.Porcentaje <= -.05]
        down = list(prices_post.index.values)
        prices_up = prices_post1[prices_post1.Porcentaje >= .05]
        up = list(prices_up.index.values)

        for i in range(13):
            word = pd.DataFrame(prices_post.iloc[i, :])
            word.columns = ["Porcentaje"]
            prices_down = word[word.Porcentaje <= -.05]
            down = list(word.index.values)
            prices_up = word[word.Porcentaje >= .05]
            up = list(word.index.values)

        titulos_ant = []
        for i in range(2):
            word = pd.DataFrame(prices_post.iloc[i, :])
            word.columns = ["Porcentaje"]
            prices_down = word[word.Porcentaje <= -.05]
            down = list(word.index.values)
            prices_up = word[word.Porcentaje >= .05]
            up = list(word.index.values)

            new_titulos = []
            new_portfolio = pd.DataFrame()
            new_portfolio["Ticker"] = data_fin.index
            new_portfolio = new_portfolio.set_index("Ticker")
            new_portfolio["Precio"] = data_close.iloc[562, :]  # 562 corresponde a 30/04/2020
            new_portfolio["Titulos anteriores"] = portfolio_1.loc[:, "Titulos"]

            for ticker in tickers:
                if ticker in down:
                    n_titulos = new_portfolio.loc[ticker, "Titulos anteriores"] * .975
                else:
                    n_titulos = new_portfolio.loc[ticker, "Titulos anteriores"]
                new_titulos.append(n_titulos)

            new_portfolio["Nuevos Titulos"] = np.floor(new_titulos)
            titulos_ant = new_portfolio["Nuevos Titulos"]
            new_portfolio["Nuevo Valor"] = new_portfolio["Nuevos Titulos"] * new_portfolio["Precio"]
            new_portfolio["Valor Venta"] = np.round(
                (new_portfolio["Titulos anteriores"] - new_portfolio["Nuevos Titulos"]) * new_portfolio["Precio"], 2)
            new_portfolio["Comisiones venta"] = new_portfolio["Valor Venta"] * 0.00125
        return new_portfolio, cash

# ---- INVERSIÓN ACTIVA ----


class ActiveInvestment:

    def __init__(self):
        pass

    @staticmethod
    def tickers():
        return FilesHandler().tickers()

    @staticmethod
    def f_portfolio1():
        return Portfolio().f_portfolio1()

    @staticmethod
    def new_port():
        return Portfolio().new_port()

    def inv_activa(self):
        tickers, data_down, data_close = self.tickers()
        portfolio_1, prices_post, prices_post1 = self.f_portfolio1()
        new_portfolio, cash = self.new_port()

        ind_pas = []
        for i in range(1, len(data_close.index)):
            if data_close.index[i - 1].month != data_close.index[i].month:
                ind_pas.append(data_close.index[i - 1])

        titulos_ant = []
        new_cash = cash
        valor_portafolio = []
        all_data = pd.DataFrame()
        acciones_venta = []
        acciones_compra = []
        comisiones_venta = []
        comisiones_compra = []

        for i in range(len(ind_pas[25:39])):
            # Obtencion de rebalanceos
            word = pd.DataFrame(prices_post.iloc[i, :])
            word.columns = ["Porcentaje"]
            prices_down = word[word.Porcentaje <= -.05]
            down = list(prices_down.index.values)
            prices_up = word[word.Porcentaje >= .05]
            up = list(prices_up.index.values)
            # Creación de nuevos portafolios
            new_titulos = []
            new_portfolio = pd.DataFrame()
            new_portfolio["Ticker"] = tickers
            new_portfolio = new_portfolio.set_index("Ticker")
            new_portfolio["Precio"] = data_close.iloc[26 + i, :]  # Periodo de pandemia empieza en iloc 26
            new_portfolio["Titulos anteriores"] = portfolio_1.loc[:, "Titulos"]

            # Venta de activos con descuento de precios
            for ticker in tickers:
                if ticker in down:
                    n_titulos = new_portfolio.loc[ticker, "Titulos anteriores"] * .975
                else:
                    n_titulos = new_portfolio.loc[ticker, "Titulos anteriores"]
                new_titulos.append(n_titulos)

            new_portfolio["Nuevos Titulos"] = np.floor(new_titulos)
            new_portfolio["Valor Venta"] = np.round(
                (new_portfolio["Titulos anteriores"] - new_portfolio["Nuevos Titulos"]) * new_portfolio["Precio"], 2)
            acciones_venta.append(new_portfolio["Valor Venta"].sum())
            new_portfolio["Comisiones venta"] = new_portfolio["Valor Venta"] * 0.00125
            comisiones_venta.append(new_portfolio["Comisiones venta"].sum())
            # Manejo de efectivo despues de venta
            new_cash = new_cash + new_portfolio["Valor Venta"].sum() - new_portfolio["Comisiones venta"].sum()

            # Compra de activos con aumento de precio
            valor_compra = []
            cash_buy = new_cash
            for ticker in tickers:
                if cash_buy >= 0 and ticker in up:
                    n_titulos = new_portfolio.loc[ticker, "Titulos anteriores"] * 1.025
                    new_portfolio.loc[ticker, "Nuevos Titulos"] = np.floor(n_titulos)
                    compra = np.round((new_portfolio.loc[ticker, "Titulos anteriores"] - new_portfolio.loc[
                        ticker, "Nuevos Titulos"]) * new_portfolio.loc[ticker, "Precio"], 2) * -1
                    if compra > cash_buy:
                        compra = 0
                else:
                    compra = 0
                valor_compra.append(compra)
                cash_buy = cash_buy - compra

            new_portfolio["Valor Compra"] = valor_compra
            acciones_compra.append(new_portfolio["Valor Compra"].sum())
            titulos_ant = new_portfolio["Nuevos Titulos"]
            new_portfolio["Comisiones Compra"] = new_portfolio["Valor Compra"] * - 0.00125
            comisiones_compra.append(new_portfolio["Comisiones Compra"].sum() * -1)
            new_portfolio["Nuevo Valor"] = new_portfolio["Nuevos Titulos"] * new_portfolio["Precio"]

            # Manejo de efectivo despues de compra
            new_cash = new_cash - new_portfolio["Valor Compra"].sum() + new_portfolio["Comisiones Compra"].sum()
            valor_portafolio.append(new_cash + new_portfolio["Nuevo Valor"].sum())
        df_activa = pd.DataFrame()
        df_activa["timestamp"] = ind_pas[24:39]
        valor_portafolio.insert(0, 1000000)
        df_activa["capital"] = valor_portafolio
        df_activa["rendimiento"] = df_activa.capital.diff() / df_activa.capital
        df_activa["rendimiento_acumulado"] = df_activa["rendimiento"].cumsum()

        # Operaciones realizadas
        df_operaciones = pd.DataFrame()
        df_operaciones["timestamp"] = ind_pas[25:39]
        df_operaciones["titulos comprados / total operacion"] = acciones_compra
        df_operaciones["titulos vendidos / total operacion"] = acciones_venta
        df_operaciones["comisiones compra"] = comisiones_compra
        df_operaciones["comisiones venta"] = comisiones_venta
        df_operaciones["comisiones mes"] = df_operaciones["comisiones compra"] + df_operaciones["comisiones venta"]
        df_operaciones["comisiones acumuladas"] = df_operaciones["comisiones mes"].cumsum()

        return df_activa, df_operaciones

# ---- MEDIDAS ----
class Medidas:

    def __init__(self):
        pass

    @staticmethod
    def inv_activa():
        return ActiveInvestment().inv_activa()

    @staticmethod
    def dur_pan():
        return PassiveInvestment().dur_pan()

    def medidas(self):
        # Se obtiene el DataFrame correspondiente a las medidas de desempeño
        df_activa, df_operaciones = self.inv_activa()
        df_pasiva_dp = self.dur_pan()
        medidas = pd.DataFrame()
        rend_m_m = [((df_activa["rendimiento"] * 12).mean() - .0429), ((df_pasiva_dp["Rendimiento"] * 12).mean() - .0429)]
        rend_m_a = [(df_activa["rendimiento_acumulado"][13]), (df_pasiva_dp["Rendimiento Acumulado"][45])]
        medidas["tipo de inversion"] = ["activa", "pasiva"]
        medidas["rend_m_m"] = rend_m_m
        medidas["rend_m_a"] = rend_m_a
        medidas.set_index("tipo de inversion")
        return medidas
