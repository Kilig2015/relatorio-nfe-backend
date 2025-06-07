import os
import uuid
import zipfile
import shutil
import io
from fastapi import FastAPI, File, UploadFile, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

# Permitir Vercel + testes locais
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml.vercel.app", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TEMP_DIR = "temp"
RESULT_DIR = "resultados"
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)


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
    job_id = str(uuid.uuid4())
    pasta_job = os.path.join(TEMP_DIR, job_id)
    os.makedirs(pasta_job, exist_ok=True)

    for file in xmls:
        if file.filename.lower().endswith('.zip'):
            with zipfile.ZipFile(file.file) as zip_ref:
                zip_ref.extractall(pasta_job)
        elif file.filename.lower().endswith('.xml'):
            with open(os.path.join(pasta_job, file.filename), "wb") as f:
                f.write(await file.read())

    background_tasks.add_task(processar_xmls, pasta_job, job_id, modo_linha_individual,
                              dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto)

    return {"id": job_id, "status": "processando"}


@app.get("/status/{job_id}")
def verificar_status(job_id: str):
    caminho_arquivo = os.path.join(RESULT_DIR, f"relatorio_{job_id}.xlsx")
    if os.path.exists(caminho_arquivo):
        return {"status": "concluido", "url": f"/download/relatorio_{job_id}.xlsx"}
    pasta = os.path.join(TEMP_DIR, job_id)
    if os.path.exists(pasta):
        return {"status": "processando"}
    return {"status": "nao_encontrado"}


@app.get("/download/relatorio_{job_id}.xlsx")
def baixar_relatorio(job_id: str):
    caminho_arquivo = os.path.join(RESULT_DIR, f"relatorio_{job_id}.xlsx")
    if os.path.exists(caminho_arquivo):
        return FileResponse(caminho_arquivo, filename=f"relatorio_nfe_{job_id}.xlsx")
    return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})


def processar_xmls(pasta, job_id, modo_individual, dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto):
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

    for root_dir, _, files in os.walk(pasta):
        for nome in files:
            if not nome.endswith(".xml"):
                continue
            try:
                caminho = os.path.join(root_dir, nome)
                tree = ET.parse(caminho)
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
                    if not modo_individual:
                        break
            except Exception as e:
                print(f"Erro ao processar {nome}: {e}")

    if not dados:
        shutil.rmtree(pasta, ignore_errors=True)
        return

    df = pd.DataFrame(dados)
    arquivo_saida = os.path.join(RESULT_DIR, f"relatorio_{job_id}.xlsx")
    df.to_excel(arquivo_saida, index=False)

    shutil.rmtree(pasta, ignore_errors=True)
