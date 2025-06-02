from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional
from zipfile import ZipFile
import tempfile
import os
import xml.etree.ElementTree as ET
import pandas as pd
import io
from datetime import datetime

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

# CORS permitido para domínio da Vercel e local
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:5173",
        "https://kxml-*.vercel.app"
    ],
    allow_origin_regex="https://kxml-[a-z0-9]+-kiligs-projects-7cfc26f2\.vercel\.app",
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
    dataInicio: Optional[str] = Form(None),
    dataFim: Optional[str] = Form(None),
    cfop: Optional[str] = Form(None),
    tipoNF: Optional[str] = Form(None),
    ncm: Optional[str] = Form(None),
    codigoProduto: Optional[str] = Form(None),
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

    try:
        for upload in xmls:
            if upload.filename.lower().endswith('.zip'):
                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = os.path.join(tmpdir, upload.filename)
                    with open(zip_path, "wb") as f:
                        f.write(await upload.read())
                    with ZipFile(zip_path, 'r') as zipf:
                        for name in zipf.namelist():
                            if name.lower().endswith('.xml'):
                                with zipf.open(name) as xmlfile:
                                    arquivos_xml.append(xmlfile.read())
            else:
                arquivos_xml.append(await upload.read())

        dados = []

        for raw in arquivos_xml:
            try:
                tree = ET.ElementTree(ET.fromstring(raw))
                root = tree.getroot()
                infNFe = root.find('.//nfe:infNFe', NS)
                protNFe = root.find('.//nfe:protNFe/nfe:infProt', NS)
                chNFe = infNFe.attrib.get('Id', '').replace('NFe', '') if infNFe is not None else ''
                xMotivo = buscar_valor_xpath(protNFe, 'xMotivo') if protNFe is not None else ''
                dets = infNFe.findall('nfe:det', NS) if infNFe is not None else []

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

                    # Aplicar filtros
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
                continue

        if not dados:
            return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

        df = pd.DataFrame(dados)
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                 headers={"Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"})

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Erro interno: {str(e)}"})
