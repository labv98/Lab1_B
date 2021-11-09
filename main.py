
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: Aquí se encuentra el main del laboratorio 1 realizado con OOP                            -- #
# -- script: main.py : python script with the main functionality                                         -- #
# -- author: labv98                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: https://github.com/labv98/Lab1_B.git                                                  -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

# ---- LIBRERÍAS ----

from functions import *

# Por si existe algún warning
import warnings
warnings.simplefilter("ignore")

# ---- TRATAMIENTO DE DATOS ----

FilesHandler().read_df().head(10)

tickers, data_down, data_close = FilesHandler().tickers()
print(data_close.head(5))

data_fin = FilesHandler().f_data_fin()
print(data_fin)

# ---- INVERSIÓN PASIVA ----

PassiveInvestment().inv_pasiva()

PassiveInvestment().ant_pan()

PassiveInvestment().dur_pan()

# ---- INVERSIÓN ACTIVA ----

df_activa, df_operaciones = ActiveInvestment().inv_activa()
print(df_activa)

print(df_operaciones)

# ---- MEDIDAS ----

Medidas().medidas()
