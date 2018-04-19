# pylint: disable=C0103
"""
high level support for doing this and that.
"""

import json
from pymongo import MongoClient

archivo = open("config.json")
config = json.load(archivo)

mongoUri = config["mongo_uri"]
documentIds = config["documentIds"]
row_format = config["row_format"]
archivo_salida = config["output_filename"]

client = MongoClient(mongoUri)
db = client.fel_cl_db

documentos = db.PersistentDocument.find({
    "Object.DocumentID" :
    {
        "$in" : documentIds
    }
})

for d in documentos:
    o = d["Object"]

    rut = ""
    td = ""
    f = ""
    estado_sii = ""
    estado_dist = ""
    comment_sii = ""
    comment_dist = ""
    if "IssuerRUT" in o:
        rut = o["IssuerRUT"]
    if "Type" in o:
        td = o["Type"]
    if "Folio" in o:
        f = o["Folio"]
    if "CL_SII" in o["Statuses"]:
        estado_sii = o["Statuses"]["CL_SII"][0]["Status"]
        comment_sii = o["Statuses"]["CL_SII"][0]["Comment"]
    if "CL_Dist" in o["Statuses"]:
        estado_dist = o["Statuses"]["CL_Dist"][0]["Status"]
        comment_dist = o["Statuses"]["CL_Dist"][0]["Comment"]

    row = row_format.format(id=o["DocumentID"],
                            rut=rut,
                            td=td,
                            f=f,
                            esii=estado_sii,
                            csii=comment_sii.replace("\\n","").replace("\\r",""),
                            edist=estado_dist,
                            cdist=comment_dist.replace("\\n","").replace("\\r",""))

    with open(archivo_salida, "a") as archivo:
        archivo.write(row)

    print(row)

print("Proceso finalizado")
