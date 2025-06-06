from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional
import os
import uuid
import zipfile
import shutil
import tempfile
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
PASTA_RESULTADOS = "resultados"
os.makedirs(PASTA_RESULTADOS, exist_ok=True)

PROCESSAMENTO = {}

def buscar_valor_xpath(base, caminho):
    partes = caminho.split('|')
    atual = base
    for i, parte in enumerate(partes):
        if atual is None:
            return ''
        if parte == '*':
            for subtag in atual:
                valor = subtag.find(f'nfe:{partes[-1]}', NS)
                if valor is not None:
                    return valor.text
            return ''
        atual = atual.find(f'nfe:{parte}', NS)
    return atual.text if atual is not None else ''

def extrair_xmls(file: UploadFile, destino: str) -> List[str]:
    zip_path = os.path.join(destino, file.filename)
    with open(zip_path, "wb") as f:
        f.write(file.file.read())
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(destino)
    os.remove(zip_path)
    xmls = []
    for root, _, files in os.walk(destino):
        for f in files:
            if f.lower().endswith(".xml"):
                xmls.append(os.path.join(root, f))
    return xmls

def processar_xmls(arquivos, filtros, modo_individual, caminho_saida):
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

    dados = []

    for path in arquivos:
        try:
            tree = ET.parse(path)
            root = tree.getroot()
            infNFe = root.find('.//nfe:infNFe', NS)
            protNFe = root.find('.//nfe:protNFe/nfe:infProt', NS)
            chNFe = infNFe.attrib.get('Id', '').replace('NFe', '')
            xMotivo = buscar_valor_xpath(protNFe, 'xMotivo') if protNFe is not None else ''

            dets = infNFe.findall('nfe:det', NS)
            for det in dets:
                linha = {}
                for campo, titulo in CAMPOS.items():
                    if campo == 'xMotivo':
                        linha[titulo] = xMotivo
                    elif campo == 'chNFe':
                        linha[titulo] = chNFe
                    elif campo.startswith('det|'):
                        linha[titulo] = buscar_valor_xpath(det, campo.replace('det|', ''))
                    else:
                        linha[titulo] = buscar_valor_xpath(infNFe, campo)

                # Filtros
                data_emi = linha["Data Emissão"][:10] if linha["Data Emissão"] else ""
                if filtros["dataInicio"] and data_emi < filtros["dataInicio"]:
                    continue
                if filtros["dataFim"] and data_emi > filtros["dataFim"]:
                    continue
                if filtros["cfop"] and linha["CFOP"] != filtros["cfop"]:
                    continue
                if filtros["tipoNF"] and linha["Tipo NF"] != ("0" if filtros["tipoNF"] == "Entrada" else "1"):
                    continue
                if filtros["ncm"] and linha["NCM"] != filtros["ncm"]:
                    continue
                if filtros["codigoProduto"] and linha["Código Produto"] != filtros["codigoProduto"]:
                    continue

                dados.append(linha)

        except Exception as e:
            print(f"Erro ao processar {path}: {e}")

    df = pd.DataFrame(dados)
    if df.empty:
        raise ValueError("Nenhum dado encontrado após aplicar os filtros.")

    df.to_excel(caminho_saida, index=False)

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
    codigoProduto: Optional[str] = Form(None)
):
    filtros = {
        "dataInicio": dataInicio,
        "dataFim": dataFim,
        "cfop": cfop,
        "tipoNF": tipoNF,
        "ncm": ncm,
        "codigoProduto": codigoProduto
    }

    uid = str(uuid.uuid4())
    pasta_tmp = os.path.join("temp", uid)
    os.makedirs(pasta_tmp, exist_ok=True)

    all_xml_paths = []
    for file in xmls:
        if file.filename.lower().endswith(".zip"):
            xml_paths = extrair_xmls(file, pasta_tmp)
            all_xml_paths.extend(xml_paths)
        elif file.filename.lower().endswith(".xml"):
            path = os.path.join(pasta_tmp, file.filename)
            with open(path, "wb") as f:
                f.write(file.file.read())
            all_xml_paths.append(path)

    saida_excel = os.path.join(PASTA_RESULTADOS, f"relatorio_{uid}.xlsx")
    PROCESSAMENTO[uid] = {"status": "processando", "arquivo": saida_excel}

    def tarefa():
        try:
            processar_xmls(all_xml_paths, filtros, modo_linha_individual, saida_excel)
            PROCESSAMENTO[uid]["status"] = "concluido"
        except Exception as e:
            PROCESSAMENTO[uid]["status"] = "erro"
            PROCESSAMENTO[uid]["erro"] = str(e)
        finally:
            shutil.rmtree(pasta_tmp, ignore_errors=True)

    background_tasks.add_task(tarefa)
    return {"id": uid}

@app.get("/status/{uid}")
async def status(uid: str):
    info = PROCESSAMENTO.get(uid)
    if not info:
        return JSONResponse(status_code=404, content={"detail": "ID não encontrado."})
    return info

@app.get("/download/{filename}")
async def download(filename: str):
    path = os.path.join(PASTA_RESULTADOS, filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=filename)
