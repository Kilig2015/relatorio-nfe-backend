from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import io
import zipfile
import os
from datetime import datetime

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ou especifique ["https://seusite.vercel.app"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def buscar_valor_xpath(base, caminho):
    partes = caminho.split('|')
    atual = base
    for parte in partes:
        if atual is None:
            return ''
        atual = atual.find(f'nfe:{parte}', NS)
    return atual.text if atual is not None else ''

@app.post("/gerar-relatorio")
async def gerar_relatorio(
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False),
    dataInicio: str = Form(""),
    dataFim: str = Form(""),
    cfop: str = Form(""),
    tipoNF: str = Form(""),
    ncm: str = Form(""),
    codigoProduto: str = Form("")
):
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

    arquivos_xml = []

    for upload in xmls:
        if upload.filename.endswith('.zip'):
            with zipfile.ZipFile(upload.file) as z:
                for nome_arquivo in z.namelist():
                    if nome_arquivo.lower().endswith('.xml'):
                        arquivos_xml.append(io.BytesIO(z.read(nome_arquivo)))
        elif upload.filename.endswith('.xml'):
            arquivos_xml.append(io.BytesIO(await upload.read()))

    dados = []
    for arquivo in arquivos_xml:
        try:
            tree = ET.parse(arquivo)
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
            print(f"Erro ao processar XML: {e}")

    if not dados:
        return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

    df = pd.DataFrame(dados)
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                              headers={"Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"})
