from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os, io, zipfile, shutil, uuid
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET

app = FastAPI()

# CORS para frontend em produção
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
RELATORIOS = {}

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

def processar_relatorio(task_id, arquivos, modo_individual, filtros):
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
            for i, det in enumerate(dets):
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
                if filtros['dataInicio'] and data_emi < filtros['dataInicio']:
                    continue
                if filtros['dataFim'] and data_emi > filtros['dataFim']:
                    continue
                if filtros['cfop'] and linha["CFOP"] != filtros['cfop']:
                    continue
                if filtros['tipoNF'] and linha["Tipo NF"] != ("0" if filtros['tipoNF'] == "Entrada" else "1"):
                    continue
                if filtros['ncm'] and linha["NCM"] != filtros['ncm']:
                    continue
                if filtros['codigoProduto'] and linha["Código Produto"] != filtros['codigoProduto']:
                    continue

                dados.append(linha)
                if not modo_individual:
                    break

        except Exception as e:
            continue

    df = pd.DataFrame(dados)
    output_path = f"relatorios/relatorio_{task_id}.xlsx"
    df.to_excel(output_path, index=False)
    RELATORIOS[task_id] = output_path

@app.post("/gerar-relatorio")
async def gerar_relatorio(
    background_tasks: BackgroundTasks,
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False),
    dataInicio: str = Form(""),
    dataFim: str = Form(""),
    cfop: str = Form(""),
    tipoNF: str = Form(""),
    ncm: str = Form(""),
    codigoProduto: str = Form("")
):
    task_id = uuid.uuid4().hex
    temp_dir = f"temp/{task_id}"
    os.makedirs(temp_dir, exist_ok=True)

    arquivos_xml = []

    for upload in xmls:
        if upload.filename.endswith('.zip'):
            with zipfile.ZipFile(upload.file) as z:
                z.extractall(temp_dir)
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith(".xml"):
                            arquivos_xml.append(os.path.join(root, file))
        elif upload.filename.endswith('.xml'):
            path = os.path.join(temp_dir, upload.filename)
            with open(path, "wb") as f:
                f.write(await upload.read())
            arquivos_xml.append(path)

    filtros = {
        'dataInicio': dataInicio,
        'dataFim': dataFim,
        'cfop': cfop,
        'tipoNF': tipoNF,
        'ncm': ncm,
        'codigoProduto': codigoProduto
    }

    background_tasks.add_task(processar_relatorio, task_id, arquivos_xml, modo_linha_individual, filtros)
    return {"task_id": task_id}

@app.get("/status/{task_id}")
def status(task_id: str):
    if task_id in RELATORIOS:
        return {"status": "pronto"}
    return {"status": "processando"}

@app.get("/download/{task_id}")
def download(task_id: str):
    if task_id in RELATORIOS:
        file_path = RELATORIOS[task_id]
        return StreamingResponse(open(file_path, "rb"), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers={"Content-Disposition": f"attachment; filename=relatorio_{task_id}.xlsx"})
    return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
