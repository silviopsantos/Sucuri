import sys, os, re
import time, math

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *

def lerArq (args):
    nomearq = args[0]
    arq = open(nomearq, "r")
    texto = []
    for linha in arq:
        texto.append(linha)
    arq.close()
    return nprocs, texto

def filtraEmail (args):
    sp1 = args
    sp = sp1[0].split("@", 1)
    if len(sp) == 2:
        aux = sp[1][:-1]
        if (aux == "hotmail.com"):
            res = sp1[0][:-1]
        else:
            res = ""
    else:
        res = ""
    return res

def imprEmail (args):
    if args[0] != "":
        print args[0]

nprocs = int(sys.argv[1])
nomearq = (sys.argv[2])

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

fp = open(nomearq, "r")

src = Source(fp)
graph.add(src)

fil = FilterTagged(filtraEmail, 1)
graph.add(fil)

ser = Serializer(imprEmail, 1)
graph.add(ser)

src.add_edge(fil, 0)
fil.add_edge(ser, 0)

ti = time.time()
sched.start()
tf = time.time()

print "Tempo de Execucao %.3f" %(tf - ti)
