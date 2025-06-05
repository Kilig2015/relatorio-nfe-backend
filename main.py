from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import io
import zipfile
import tempfile
import uuid
import os
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
TEMP_DIR = "relatorios"

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

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
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False),
    dataInicio: str = Form(None),
    dataFim: str = Form(None),
    cfop: str = Form(None),
    tipoNF: str = Form(None),
    ncm: str = Form(None),
    codigoProduto: str = Form(None),
):
    try:
        arquivos = []

        for arquivo in xmls:
            if arquivo.filename.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(await arquivo.read())) as zip_ref:
                    for name in zip_ref.namelist():
                        if name.endswith('.xml'):
                            with zip_ref.open(name) as f:
                                arquivos.append(f.read())
            elif arquivo.filename.endswith('.xml'):
                arquivos.append(await arquivo.read())

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

        for conteudo in arquivos:
            try:
                tree = ET.ElementTree(ET.fromstring(conteudo))
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

            except Exception as e:
                print(f"Erro ao processar um XML: {e}")

        if not dados:
            return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

        df = pd.DataFrame(dados)
        relatorio_id = str(uuid.uuid4()).replace('-', '')
        caminho = os.path.join(TEMP_DIR, f"relatorio_{relatorio_id}.xlsx")
        df.to_excel(caminho, index=False)

        return {"id": relatorio_id, "url": f"/download/{relatorio_id}"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Erro no servidor: {str(e)}"})

@app.get("/download/{relatorio_id}")
async def download(relatorio_id: str):
    caminho = os.path.join(TEMP_DIR, f"relatorio_{relatorio_id}.xlsx")
    if not os.path.exists(caminho):
        return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
    return StreamingResponse(
        open(caminho, "rb"),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=relatorio_{relatorio_id}.xlsx"}
    )
