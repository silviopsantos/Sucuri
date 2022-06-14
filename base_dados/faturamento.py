import os, sys, time
import pandas as pd

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

t0 = time.time()
path = '/home/silvio/Sucuri/base_dados'

planilhas = os.listdir(path)

def base_dados(planilhas):
    
    return planilhas

def loja_shopping(args):
    planilha = args[0]
    if "xlsx" in planilha:
        tabela = pd.read_excel(planilha)
        loja = planilha.replace('.xlsx', '')
        return loja

def imprime_loja(args):
    loja = args[0]
    print 'A loja do',
    print loja
    print '\n'

def calcular_faturamento(args):
    planilha = args[0]
    if "xlsx" in planilha:
        tabela = pd.read_excel(planilha)
        faturamento = float(tabela["Valor Final"].sum())
        return faturamento

def imprime_faturamento(args):
    faturamento = args[0]
    print 'Faturou R$',
    print faturamento
    print '\n'
    

def faturamento_total():
    fat_rede = 0
    for planilha in planilhas:
        if "xlsx" in planilha:
            tabela = pd.read_excel(planilha)
            faturamento = tabela["Valor Final"].sum()
            fat_rede = fat_rede + faturamento
    print"\nFaturamento total da Rede foi de R$ %.2f" %fat_rede



nprocs = int(sys.argv[1])
lista_planilhas = base_dados(planilhas)

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

feed_plan = Source(lista_planilhas)
loja_plan = FilterTagged(loja_shopping, 1)
calc_plan = FilterTagged(calcular_faturamento, 1)  
ploja = Serializer(imprime_loja, 1)
pfaturamento = Serializer(imprime_faturamento, 1)

graph.add(feed_plan)
graph.add(loja_plan)
graph.add(calc_plan)
graph.add(ploja)
graph.add(pfaturamento)

feed_plan.add_edge(loja_plan, 0)
feed_plan.add_edge(calc_plan, 0)
loja_plan.add_edge(ploja, 0)
calc_plan.add_edge(pfaturamento, 0)

t1 = time.time()
sched.start()
t2 = time.time()

faturamento_total()

t3 = time.time()

print "\nTempo de execucao em paralelo do Programa %.3f" %(t2-t1)
print "\nTempo de execucao total do Programa %.3f\n" %(t3-t0)
