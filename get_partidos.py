import os, sys
from string import Template
from multiprocessing import Pool

partidos = ["dem",
"novo",
"pen",
"pc_do_b",
"pcb",
"pco",
"pdt",
"phs",
"pmdb",
"pmn",
"pp",
"ppl",
"pps",
"pr",
"prb",
"pros",
"prp",
"prtb",
"psb",
"psc",
"psd",
"psdb",
"psdc",
"psl",
"psol",
"pstu",
"pt",
"pt_do_b",
"ptb",
"ptc",
"ptn",
"pv",
"rede",
"sd"]

link = "http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_%s_pb.zip"
cmd = Template("curl http://agencia.tse.jus.br/estatistica/sead/eleitorado/filiados/uf/filiados_${p}_pb.zip -o zip/${p}_pb.zip && unzip -p zip/${p}_pb.zip aplic/sead/lista_filiados/uf/filiados_${p}_pb.csv > csv/${p}_pb.csv")

def download(partido):
    os.system(cmd.substitute(p=partido))

def get():
    pool = Pool(5)
    print pool.map(download, partidos)

get()
