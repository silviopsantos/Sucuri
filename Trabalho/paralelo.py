# -*- coding: utf-8 -*-
"""
Created on Sun May  1 02:24:07 2022

@author: Silvio Santos
"""

import time, os
import pandas as pd

tempo_inicial = time.time()
arquivos = os.listdir()
#tabela_final = pd.DataFrame()
fat_rede = 0
for arquivo in arquivos:
    if "xlsx" in arquivo:
        tabela = pd.read_excel(arquivo)
        faturamento = tabela["Valor Final"].sum()
        fat_rede = fat_rede + faturamento
        print(f"Faturamento da Loja {arquivo.replace('.xlsx', '')} foi de R${faturamento:.2f}")

print(f"\nFaturamento total da rede foi de R${fat_rede:.2f}")
print(f"\nDemorou: {time.time() - tempo_inicial:.3f}")

#Programa em paralelo
print (f'\nPrograma em Paralelo\n')
from joblib import Parallel, delayed

tempo_inicial = time.time()
arquivos = os.listdir()

def calcular_faturamento(arquivo):
    fat_rede = 0
    if "xlsx" in arquivo:
        tabela = pd.read_excel(arquivo)
        faturamento = tabela["Valor Final"].sum()
        fat_rede = fat_rede + faturamento
        return f"Faturamento da Loja {arquivo.replace('.xlsx', '')} foi de R${faturamento:.2f}"

resultado = Parallel(n_jobs=2)(delayed(calcular_faturamento)(arquivo) for arquivo in arquivos)
print(resultado)
print(f"\nFaturamento total da rede foi de R${fat_rede:.2f}")
print(f"\nDemorou: {time.time() - tempo_inicial:.3f}")
