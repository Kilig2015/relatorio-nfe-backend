from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import xmltodict
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/gerar-relatorio")
async def gerar_relatorio(
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False)
):
    linhas = []

    for xml_file in xmls:
        conteudo = await xml_file.read()
        try:
            xml_dict = xmltodict.parse(conteudo)
            nfe = xml_dict.get("nfeProc") or xml_dict.get("NFe") or xml_dict
            ide = nfe["NFe"]["infNFe"]["ide"]
            emit = nfe["NFe"]["infNFe"]["emit"]
            det = nfe["NFe"]["infNFe"]["det"]

            if not isinstance(det, list):
                det = [det]

            for item in det:
                produto = item["prod"]
                linha = {
                    "refNFe": ide.get("nNF", ""),
                    "emitente": emit.get("xNome", ""),
                    "dataEmissao": ide.get("dhEmi", ide.get("dEmi", "")),
                    "produto": produto.get("xProd", ""),
                    "quantidade": produto.get("qCom", ""),
                    "valorUnitario": produto.get("vUnCom", ""),
                    "valorTotal": produto.get("vProd", ""),
                    "NCM": produto.get("NCM", "")
                }

                if modo_linha_individual:
                    linhas.append(linha)
            if not modo_linha_individual:
                resumo = {
                    "refNFe": ide.get("nNF", ""),
                    "emitente": emit.get("xNome", ""),
                    "dataEmissao": ide.get("dhEmi", ide.get("dEmi", "")),
                    "totalItens": len(det),
                    "valorTotalNota": sum(float(i["prod"]["vProd"]) for i in det)
                }
                linhas.append(resumo)
        except Exception as e:
            linhas.append({"erro": f"Erro ao processar XML: {e}"})

    df = pd.DataFrame(linhas)
    return {
        "status": "ok",
        "columns": df.columns.tolist(),
        "rows": df.to_dict(orient="records")
    }
