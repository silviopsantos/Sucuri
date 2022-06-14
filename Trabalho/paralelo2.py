# -*- coding: utf-8 -*-
"""
Created on Sun May  1 02:24:07 2022

@author: Silvio Santos
"""

import time, os
import pandas as pd

path = '/home/silvio/Sucuri/paralelo'
tempo_inicial = time.time()
arquivos = os.listdir(path)
#tabela_final = pd.DataFrame()
fat_rede = 0
base = []
for arquivo in arquivos:
    if "xlsx" in arquivo:
        tabela = pd.read_excel(arquivo)
        faturamento = tabela["Valor Final"].sum()
        fat_rede = fat_rede + faturamento
        print"Faturamento da Loja %s" %(arquivo.replace('.xlsx', '')),  "foi de R$ %.2f" %faturamento
        base.append(arquivo.replace('.xlsx', ''))
        base.append(faturamento)

print"\nFaturamento total da rede foi de R$ %.2f\n" %fat_rede
print"Demorou: %.3f" %(time.time() - tempo_inicial)
print (base)

#Programa em paralelo
print '\nPrograma em Paralelo\n'
from joblib import Parallel, delayed

tempo_inicial = time.time()
arquivos = os.listdir(path)

def calcular_faturamento(arquivo):
    fat_rede = 0
    if "xlsx" in arquivo:
        tabela = pd.read_excel(arquivo)
        faturamento = tabela["Valor Final"].sum()
        fat_rede = fat_rede + faturamento
        return "Faturamento da Loja %s" %(arquivo.replace('.xlsx', '')),  "foi de R$ %.2f" %faturamento

resultado = Parallel(n_jobs=1)(delayed(calcular_faturamento)(arquivo) for arquivo in arquivos)
print(resultado)
print"\nFaturamento total da rede foi de R$ %.2f\n" %fat_rede
print"Demorou: %.3f" %(time.time() - tempo_inicial)

