import os, time
import sys

sys.path.append(os.environ['PYDFHOME'])
from pyDF import *


from PIL import Image

save_dir = 'imagem_comprimida'

path = '/home/silvio/Sucuri/Trabalho/Comprimir'

def lista_imgens(path):
    fnames = os.listdir(path)
    fname = []
    for name in fnames:
        if name.endswith('jpeg') or name.endswith('jpg') or name.endswith('bmp'):
            fname.append(name)
        fname.sort()
    return fname


def comprimimagem(args):
    comprimimg = args[0]
    img = Image.open(comprimimg)
    comprimimg = save_dir + '/' +  comprimimg.split('.')[0] + '_comprimido' + '.jpeg'
    img.save(comprimimg, optimize = True, quality = 10)
    return comprimimg


def print_name(args):
    fname = args[0]
    print "Imagens Comprimidas:\n %s" %fname


nprocs = int(sys.argv[1])
file_list = lista_imgens(path)

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)



feed_files = Source(file_list)

comprime_file = FilterTagged(comprimimagem, 1)  

pname = Serializer(print_name, 1)


graph.add(feed_files)
graph.add(comprime_file)
graph.add(pname)


feed_files.add_edge(comprime_file, 0)
comprime_file.add_edge(pname, 0)

ti = time.time()
sched.start()
tf = time.time()

print "Tempo de Execucao %.3f" %(tf - ti)