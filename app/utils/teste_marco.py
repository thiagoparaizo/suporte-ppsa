import pandas as pd
import streamlit as st
from pymongo import MongoClient
from bson.decimal128 import Decimal128
import decimal

MONGO_URI = "mongodb://sgpp:DfF&33CClPcE@ca1c2a82-3e02-4a81-9cea-90d338e9bc7e-0.c38qvnlz04atmdpus310.databases.appdomain.cloud:31399,ca1c2a82-3e02-4a81-9cea-90d338e9bc7e-1.c38qvnlz04atmdpus310.databases.appdomain.cloud:31399,ca1c2a82-3e02-4a81-9cea-90d338e9bc7e-2.c38qvnlz04atmdpus310.databases.appdomain.cloud:31399/?authSource=admin&replicaSet=replset&ssl=true&tlsCAFile=C:\\\\Users\\\\thiago.paraizo\\\\Documents\\\\Scala\\\\PPSA\\\\ce7a62b2-457a-4740-8145-b897b98646bf.pem"
client = MongoClient(MONGO_URI)
db = client.sgppServices  # database name


contrato_cru = db.contrato_entity.find_one({"nome": "Sépia"})


def convert_decimal(doc):
    if isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, Decimal128):
                doc[key] = value.to_decimal()
            elif isinstance(value, (dict, list)):
                doc[key] = convert_decimal(value)
    elif isinstance(doc, list):
        doc = [convert_decimal(item) for item in doc]
    return doc

processed_data = convert_decimal(contrato_cru)

if isinstance(processed_data, dict):
    df = pd.DataFrame([processed_data])
else:
    df = pd.DataFrame(processed_data)

st.write("Contrato Data:")
st.dataframe(df)

st.json(processed_data)