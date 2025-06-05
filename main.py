from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List
import uuid
import zipfile
import os
import io
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, defina domínios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

PASTA_PROCESSANDO = "processando"
PASTA_PRONTOS = "prontos"

os.makedirs(PASTA_PROCESSANDO, exist_ok=True)
os.makedirs(PASTA_PRONTOS, exist_ok=True)

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


def buscar_valor_xpath(base, caminho):
    partes = caminho.split('|')
    atual = base
    for parte in partes[:-1]:
        atual = atual.find(f'nfe:{parte}', NS)
        if atual is None:
            return ''
    final = atual.find(f'nfe:{partes[-1]}', NS)
    return final.text if final is not None else ''


def extrair_arquivos_xml(arquivo_zip: UploadFile) -> List[bytes]:
    conteudo = arquivo_zip.file.read()
    with zipfile.ZipFile(io.BytesIO(conteudo)) as zip_file:
        return [zip_file.read(name) for name in zip_file.namelist() if name.endswith(".xml")]


def processar_relatorio(arquivos_bytes, modo_linha_individual, dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto, output_path):
    dados = []

    try:
        for xml_bytes in arquivos_bytes:
            try:
                tree = ET.ElementTree(ET.fromstring(xml_bytes))
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

                    if not modo_linha_individual:
                        break
            except Exception as e:
                print(f"[Erro individual XML]: {e}")
    except Exception as e:
        print(f"[Erro geral processamento]: {e}")

    df = pd.DataFrame(dados)
    df.to_excel(output_path, index=False)


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
    codigoProduto: str = Form(""),
):
    task_id = str(uuid.uuid4())
    output_path = os.path.join(PASTA_PRONTOS, f"relatorio_{task_id}.xlsx")

    arquivos_bytes = []

    for xml_file in xmls:
        if xml_file.filename.endswith(".zip"):
            arquivos_bytes.extend(extrair_arquivos_xml(xml_file))
        elif xml_file.filename.endswith(".xml"):
            arquivos_bytes.append(xml_file.file.read())

    if not arquivos_bytes:
        return JSONResponse(status_code=400, content={"detail": "Nenhum arquivo XML válido encontrado."})

    background_tasks.add_task(
        processar_relatorio,
        arquivos_bytes,
        modo_linha_individual,
        dataInicio,
        dataFim,
        cfop,
        tipoNF,
        ncm,
        codigoProduto,
        output_path
    )

    return {"id": task_id}


@app.get("/status/{task_id}")
async def verificar_status(task_id: str):
    caminho = os.path.join(PASTA_PRONTOS, f"relatorio_{task_id}.xlsx")
    if os.path.exists(caminho):
        return {"status": "pronto", "url": f"/download/relatorio_{task_id}.xlsx"}
    return {"status": "processando"}


@app.get("/download/relatorio_{task_id}.xlsx")
async def baixar_relatorio(task_id: str):
    caminho = os.path.join(PASTA_PRONTOS, f"relatorio_{task_id}.xlsx")
    if os.path.exists(caminho):
        return FileResponse(caminho, filename=f"relatorio_nfe.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
