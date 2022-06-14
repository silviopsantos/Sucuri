import os, sys, time
import pandas as pd

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

t0 = time.time()
path = '/home/silvio/Sucuri/paralelo'
fat_rede = 0
planilhas = os.listdir(path)

def base_dados(planilhas):
    
    return planilhas

def calcular_faturamento(args):
    planilha = args[0]
    if "xlsx" in planilha:
        tabela = pd.read_excel(planilha)
        #loja = planilha.replace('.xlsx', '')
        faturamento = float(tabela["Valor Final"].sum())
        #fat_rede = fat_rede + faturamento
        return faturamento

def imprime_faturamento(arg):
    faturamento = arg
    print 'O faturamento foi R$',
    print faturamento[0]


nprocs = int(sys.argv[1])
lista_planilhas = base_dados(planilhas)

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

feed_plan = Source(lista_planilhas)
calc_plan = FilterTagged(calcular_faturamento, 1)  
ploja = Serializer(imprime_faturamento, 1)

graph.add(feed_plan)
graph.add(calc_plan)
graph.add(ploja)

feed_plan.add_edge(calc_plan, 0)
calc_plan.add_edge(ploja, 0)


sched.start()
t1 = time.time()

print "Tempo de execucao %.3f" %(t1-t0)
