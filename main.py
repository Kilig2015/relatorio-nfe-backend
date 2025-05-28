
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font
from typing import List

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
async def gerar_relatorio(xmls: List[UploadFile] = File(...), modo_linha_individual: bool = Form(False)):
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

    for xml in xmls:
        try:
            tree = ET.parse(xml.file)
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
                dados.append(linha)
                if not modo_linha_individual:
                    break

        except Exception as e:
            print(f"Erro ao processar XML: {e}")

    df = pd.DataFrame(dados)
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Relatório NFe"
    colunas = list(df.columns)
    ws.append(colunas)
    for _, row in df.iterrows():
        ws.append([row.get(col, '') for col in colunas])
    for cell in ws[1]:
        cell.font = Font(bold=True)
    wb.save(output)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"
    })
