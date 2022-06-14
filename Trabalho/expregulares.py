import sys, os, re
import time, math

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

"""def lista_arq(args):
    nomearqs = args[0]
    f = open(nomearqs, "r")
    arquivo = []
    for linha in f:
        arquivo.append("/home/silvio/Sucuri/Trabalho/textos/" + linha.strip())
    f.close()
    return arquivo """

def lerArquivo(args):
    #nomearq = args[1]
    arq = narq
    texto = []
    for linha in arq:
        texto.append(linha)
    arq.close()
    return texto

def procuraExpreg(args):
    expr = args[2]
    cont = 0
    achou = False
    for linha in texto:
        if expr in linha:
            achou = True
            cont = cont + 1
    return achou, cont

def imprimeResultado(args):
    if achou != False:
        print "A expressao regular foi encontrada", cont, "vezes no texto."
    else:
        print "A expressao regualar nao foi encontrada no texto"


nprocs = int(sys.argv[1])
expr = (sys.argv[2])
nomearq = "./textos/Dark_Mad_Girl.txt"

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)
narq = open(nomearq, "r")

src = Source(narq)
graph.add(src)

pexp = FilterTagged(procuraExpreg, 1)
graph.add(pexp)

imp_result = Serializer(imprimeResultado, 1)
graph.add(imp_result)

src.add_edge(pexp, 0)
pexp.add_edge(imp_result, 0)

ti = time.time()
sched.start()
tf = time.time()

print "Tempo de Execucao %.3f" %(tf - ti)

   
