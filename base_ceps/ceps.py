from glob import glob
from time import sleep
import requests
import pandas as pd
from base_ceps.configs import TOKEN

def request_cep(cep, token):
    assert isinstance(cep, str), "cep deve ser uma string"
    assert isinstance(token, str), "token deve ser string"
    url = f"https://www.cepaberto.com/api/v3/cep?cep={cep}"
    headers = {"Authorization": f"Token token={token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    sleep(1)
    return resp

def parse_json_cep(resp):
    cep_json = resp.json()
    try:
        cep_dict = {
            "NM_ESTADO": cep_json["estado"]["sigla"],
            "NM_CIDADE": cep_json["cidade"]["nome"],
            "ID_IBGE": cep_json["cidade"]["ibge"],
            "ID_DDD": cep_json["cidade"]["ddd"],
            "NM_BAIRRO": cep_json["bairro"],
            "NM_LOGRADOURO": cep_json["logradouro"],
            "ID_CEP": cep_json["cep"],
            "NU_LATI_RUA": cep_json["latitude"],
            "NU_LONG_RUA": cep_json["longitude"],
            "NU_ALTITUDE": cep_json["altitude"]
        }
    except:
        cep_dict = {
            "NM_ESTADO": None,
            "NM_CIDADE": None,
            "ID_IBGE": None,
            "ID_DDD": None,
            "NM_BAIRRO": None,
            "NM_LOGRADOURO": None,
            "ID_CEP": None,
            "NU_LATI_RUA": None,
            "NU_LONG_RUA": None,
            "NU_ALTITUDE": None
        }
    return cep_dict


def busca_ceps(lista_de_ceps, token):
    for cep in lista_de_ceps:
        resp = request_cep(cep, token)
        cep_dict = parse_json_cep(resp)
        cep_df = pd.DataFrame(cep_dict, index=[0])
        cep_df.to_csv(f"./data/ceps/cep_{cep}.csv", index=False)
        

def ceps_df(pasta_glob):
    ceps = glob(pasta_glob)
    lista_dfs_ceps = []
    for cep in ceps:
        df_cep = pd.read_csv(cep)
        if df_cep["ID_CEP"].isna()[0]:
            df_cep["ID_CEP"] = cep[16:24]
        lista_dfs_ceps.append(df_cep)
    df_ceps = pd.concat(lista_dfs_ceps)
    return df_ceps

def main():
    cep_unicos = pd.read_csv("./data/cep_unicos.csv")["NUM_CEP_LOGRADOURO_FAM"].astype(str).tolist()
    ceps_ja_salvos = [s[16:24] for s in glob("./data/ceps/cep_*.csv")]
    ceps_nao_salvos = [cep for cep in cep_unicos if cep not in ceps_ja_salvos]
    busca_ceps(ceps_nao_salvos, TOKEN)
    

if __name__ == "__main__":
    df = ceps_df("./data/ceps/cep_*.csv")
        