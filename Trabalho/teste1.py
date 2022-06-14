import os, sys, re
"""
nomearqs = "arquivos.txt"
f = open(nomearqs, "r")
arquivo = []
for linha in f:
    arquivo.append("/home/silvio/Sucuri/Trabalho/textos/" + linha.strip())
f.close()
print(arquivo)
"""
nomearq = "./emails1.txt"
arq = open(nomearq, "r")
texto = []
for linha in arq:
    texto.append(linha)
arq.close()
"""
expr = '@'
cont = 0
achou = False
for linha in texto:
    if expr in linha:
        achou = True
        cont = cont + 1

print(texto)
print(achou, cont)
if achou != True:
    print "A expressao regualar nao foi encontrada no texto"
else:
    print "A expressao regular foi encontrada", cont, "vezes no texto."
"""
print texto
email = []

for i in range(len(texto)):
    sp0 = re.findall('\S+@\S+', texto[i])
    print sp0
    sp0 = str(sp0)
    sp0 = re.sub("\]|\'|\[","", sp0)
    print sp0
    if (sp0 != ""):
        email.append(sp0)
    
print email

sp1 = email

for i in range(len(email)):
    sp = sp1[i].split("@", 1)
    print sp
    aux = sp[1][:]
    print aux
    if (aux == "hotmail.com"):
        res = sp1[0][:]
    print res
   
    
