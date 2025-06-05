from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from uuid import uuid4
import os, zipfile, io, shutil
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

app = FastAPI()

# CORS para acesso do frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Substitua pelo domínio do frontend se quiser restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Diretórios
TEMP_DIR = "processando"
RESULT_DIR = "prontos"
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

def buscar_valor_xpath(base, caminho):
    partes = caminho.split("|")
    atual = base
    for parte in partes:
        if atual is None:
            return ""
        atual = atual.find(f"nfe:{parte}", NS)
    return atual.text if atual is not None else ""

CAMPOS = {
    "ide|nNF": "Número NF",
    "ide|serie": "Série",
    "ide|dhEmi": "Data Emissão",
    "ide|tpNF": "Tipo NF",
    "emit|CNPJ": "CNPJ Emitente",
    "emit|xNome": "Emitente",
    "det|prod|cProd": "Código Produto",
    "det|prod|xProd": "Descrição Produto",
    "det|prod|CFOP": "CFOP",
    "det|prod|NCM": "NCM",
    "det|prod|qCom": "Quantidade",
    "det|prod|vUnCom": "Valor Unitário",
    "det|prod|vProd": "Valor Total Item",
    "xMotivo": "Motivo Retorno",
    "chNFe": "Chave de Acesso",
}

def processar_xmls_e_gerar_excel(files: List[UploadFile], modo_individual, dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto, relatorio_id):
    dados = []

    for file in files:
        try:
            tree = ET.parse(file.file)
            root = tree.getroot()
            infNFe = root.find(".//nfe:infNFe", NS)
            protNFe = root.find(".//nfe:protNFe/nfe:infProt", NS)
            chNFe = infNFe.attrib.get("Id", "").replace("NFe", "")
            xMotivo = buscar_valor_xpath(protNFe, "xMotivo") if protNFe is not None else ""

            dets = infNFe.findall("nfe:det", NS)
            for det in dets:
                linha = {}
                for campo, titulo in CAMPOS.items():
                    if campo == "xMotivo":
                        linha[titulo] = xMotivo
                    elif campo == "chNFe":
                        linha[titulo] = chNFe
                    elif campo.startswith("det|"):
                        linha[titulo] = buscar_valor_xpath(det, campo.replace("det|", ""))
                    else:
                        linha[titulo] = buscar_valor_xpath(infNFe, campo)

                # Filtros
                data_emi = linha["Data Emissão"][:10] if linha["Data Emissão"] else ""
                if dataInicio and data_emi < dataInicio:
                    continue
                if dataFim and data_emi > dataFim:
                    continue
                if cfop and linha["CFOP"] != cfop:
                    continue
                if tipoNF and linha["Tipo NF"] != ("0" if tipoNF == "Entrada" else "1"):
                    continue
                if ncm and linha["NCM"] != ncm:
                    continue
                if codigoProduto and linha["Código Produto"] != codigoProduto:
                    continue

                dados.append(linha)

    df = pd.DataFrame(dados)
    output_path = os.path.join(RESULT_DIR, f"{relatorio_id}.xlsx")
    df.to_excel(output_path, index=False)

@app.post("/gerar-relatorio")
async def gerar_relatorio(
    background_tasks: BackgroundTasks,
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False),
    dataInicio: Optional[str] = Form(None),
    dataFim: Optional[str] = Form(None),
    cfop: Optional[str] = Form(None),
    tipoNF: Optional[str] = Form(None),
    ncm: Optional[str] = Form(None),
    codigoProduto: Optional[str] = Form(None),
):
    relatorio_id = uuid4().hex

    # Salva arquivos temporários
    arquivos_salvos = []
    for file in xmls:
        filename = file.filename
        if filename.endswith(".zip"):
            zip_bytes = await file.read()
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                for nome in z.namelist():
                    if nome.endswith(".xml"):
                        with z.open(nome) as f:
                            temp_path = os.path.join(TEMP_DIR, f"{uuid4().hex}.xml")
                            with open(temp_path, "wb") as out:
                                out.write(f.read())
                            arquivos_salvos.append(open(temp_path, "rb"))
        else:
            temp_path = os.path.join(TEMP_DIR, f"{uuid4().hex}_{filename}")
            with open(temp_path, "wb") as out:
                content = await file.read()
                out.write(content)
            arquivos_salvos.append(open(temp_path, "rb"))

    background_tasks.add_task(
        processar_xmls_e_gerar_excel,
        arquivos_salvos,
        modo_linha_individual,
        dataInicio,
        dataFim,
        cfop,
        tipoNF,
        ncm,
        codigoProduto,
        relatorio_id
    )

    return {"id": relatorio_id}

@app.get("/status/{relatorio_id}")
async def status_relatorio(relatorio_id: str):
    path = os.path.join(RESULT_DIR, f"{relatorio_id}.xlsx")
    if os.path.exists(path):
        return {"status": "pronto", "url": f"https://relatorio-nfe-backend.onrender.com/download/{relatorio_id}.xlsx"}
    return {"status": "processando"}

@app.get("/download/{nome}")
async def download(nome: str):
    path = os.path.join(RESULT_DIR, nome)
    if os.path.exists(path):
        return FileResponse(path, filename=nome, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
