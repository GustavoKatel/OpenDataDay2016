import csv, sys
from multiprocessing import Pool

folhalist = []
partidolist = []

def createEntryFolha(c0="", c1="", c2="", c3="", c4="", c5="", c6="", c7="", c8=""):
    return dict(
        # de_poder=c0,  # 0
        # de_OrgaoLotacao=c1,  # 1
        # no_cargo=c2,  # 2
        # tp_cargo=c3,  # 3
        # nu_cpf=c4,  # 4
        # no_Servidor=c5,  # 5
        # dt_mesano_inicial=c6,  # 6
        # dt_mesano_final=c7,  # 6
        # dt_Admissao=c8  # 7

        cd_ugestora=c0,  # 0
        de_ugestora=c1,  # 1
        de_cargo=c2,  # 2
        de_tipocargo=c3,  # 3
        cd_cpf=c4,  # 4
        dt_mesano_inicial=c5,  # 5
        dt_mesano_final=c6,  # 5
        no_servidor=c7,  # 6
        de_uorcamentaria=c8  # 7
    )

def createEntryPartido(insc, nome, partido, municipio, data_filiacao, situacao,
                        tipo, processamento, data_cancelamento,
                        data_regularizacao, motivo, folha_index=-1):
    return dict(
        insc=insc,
        nome=nome,
        partido=partido,
        municipio=municipio,
        data_filiacao=data_filiacao,
        situacao=situacao,
        tipo=tipo,
        processamento=processamento,
        data_cancelamento=data_cancelamento,
        data_regularizacao=data_regularizacao,
        motivo=motivo,

        folha_index=folha_index
    )

def check(args):
    matches = 0
    for curentry in folhalist:
        for i in range(args[0], args[1]):
            if curentry["no_servidor"] == partidolist[i]["nome"]:
                try:
                    partidolist[i]["folha_index"] = index
                    matches += 1
                except:
                    pass
    return matches

def crossdata(partido, tipos="all"):
    folhafd = open('TCE-PB-SAGRES-Folha_Pessoal_Esfera_Municipal.csv', 'r')
    partidofd = open('csv/%s_pb.csv' % partido, 'r')

    folhareader = csv.reader(folhafd, delimiter='|')
    partidoreader = csv.reader(partidofd, delimiter=';')

    partidoreader.next()
    lines = 0
    index = -1
    for row in partidoreader:
        if not row[7] == "20516":
            continue

        curentry = createEntryPartido(
            insc=row[2],
            nome=row[3],
            partido=row[4],
            municipio=row[8],
            data_filiacao=row[1],
            situacao=row[12],
            tipo=row[13],
            processamento=row[14],
            data_cancelamento=row[16],
            data_regularizacao=row[17],
            motivo=row[18]
        )

        partidolist.append(curentry)

    index = -1
    lines = 0
    matches = 0
    folhareader.next()
    for row in folhareader:
        lines += 1
        if index == -1:
            curentry = createEntryFolha(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[6], row[7])
            index = len(folhalist)
            folhalist.append(curentry)
        else:
            curentry = folhalist[index]
            if not curentry["no_servidor"] == row[6]:
                curentry = createEntryFolha(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[6], row[7])
                index = len(folhalist)
                folhalist.append(curentry)
            else:
                folhalist[index]["dt_mesano_final"] = row[5]

    # print "starting threads..."
    # threads = 4
    # length = len(partidolist) / threads
    # count = 0
    # chunks = []
    # while count < len(partidolist):
    #     count += length
    #     chunks.append([count, count+length])
    # pool = Pool(threads)
    # matches = sum(pool.map(check, chunks))

    print "matching..."

    outputfd = open('output.csv', 'wb')
    outputwriter = csv.writer(outputfd, delimiter=';')

    outputwriter.writerow([
        'folha_cd_ugestora',
        'folha_de_ugestora',
        'folha_de_cargo',
        'folha_de_tipocargo',
        'folha_cd_cpf',
        'folha_dt_mesano_inicial',
        'folha_dt_mesano_final',
        'folha_no_servidor',
        'folha_de_uorcamentaria',

        "filiacao_insc",
        "filiacao_nome",
        "filiacao_partido",
        "filiacao_municipio",
        "filiacao_data_filiacao",
        "filiacao_situacao",
        "filiacao_tipo",
        "filiacao_processamento",
        "filiacao_data_cancelamento",
        "filiacao_data_regularizacao",
        "filiacao_motivo"
        ]
    )

    for curentry in folhalist:
        for i in range(len(partidolist)):
            if curentry["no_servidor"] == partidolist[i]["nome"]:
                partidolist[i]["folha_index"] = index
                matches += 1
                outputwriter.writerow([
                    curentry['cd_ugestora'],
                    curentry['de_ugestora'],
                    curentry['de_cargo'],
                    curentry['de_tipocargo'],
                    curentry['cd_cpf'],
                    curentry['dt_mesano_inicial'],
                    curentry['dt_mesano_final'],
                    curentry['no_servidor'],
                    curentry['de_uorcamentaria'],

                    partidolist[i]["insc"],
                    partidolist[i]["nome"],
                    partidolist[i]["partido"],
                    partidolist[i]["municipio"],
                    partidolist[i]["data_filiacao"],
                    partidolist[i]["situacao"],
                    partidolist[i]["tipo"],
                    partidolist[i]["processamento"],
                    partidolist[i]["data_cancelamento"],
                    partidolist[i]["data_regularizacao"],
                    partidolist[i]["motivo"]
                ])
                print "matches: %s - '%s'='%s'" % (matches, partidolist[i]["nome"], curentry["no_servidor"])

    print ""

    print "lines: %s" % lines
    print "items: %s" % len(folhalist)
    print "matches: %s" % matches



crossdata('pt', "all")
