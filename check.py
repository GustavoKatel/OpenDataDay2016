import csv, sys, os

def crossdata(partido, tipos="all"):
    partidofd = open('csv/%s_pb.csv' % partido, 'r')

    partidoreader = csv.reader(partidofd, delimiter=';')

    partidoreader.next()

    for row in partidoreader:
        if not row[7] == "20516":
            continue

        os.system("grep \"%s\" TCE-PB-SAGRES-Folha_Pessoal_Esfera_Municipal.csv" % row[3])


crossdata('pt', "all")
