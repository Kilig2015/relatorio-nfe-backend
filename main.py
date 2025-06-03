from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List
import zipfile, os, shutil, uuid
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
TEMP_DIR = "temp_uploads"
os.makedirs(TEMP_DIR, exist_ok=True)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml.vercel.app"],
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
    dataInicio: str = Form(None),
    dataFim: str = Form(None),
    cfop: str = Form(None),
    tipoNF: str = Form(None),
    ncm: str = Form(None),
    codigoProduto: str = Form(None)
):
    session_id = str(uuid.uuid4())
    session_folder = os.path.join(TEMP_DIR, session_id)
    os.makedirs(session_folder, exist_ok=True)

    # Extrair arquivos XML (zip ou individuais)
    for xml in xmls:
        if xml.filename.endswith('.zip'):
            with zipfile.ZipFile(xml.file) as zipf:
                zipf.extractall(session_folder)
        elif xml.filename.endswith('.xml'):
            with open(os.path.join(session_folder, xml.filename), 'wb') as f:
                f.write(await xml.read())

    arquivos_xml = []
    for root, _, files in os.walk(session_folder):
        arquivos_xml.extend([
            os.path.join(root, f) for f in files if f.lower().endswith('.xml')
        ])

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

    for caminho in arquivos_xml:
        try:
            tree = ET.parse(caminho)
            root = tree.getroot()
            infNFe = root.find('.//nfe:infNFe', NS)
            protNFe = root.find('.//nfe:protNFe/nfe:infProt', NS)
            chNFe = infNFe.attrib.get('Id', '').replace('NFe', '')
            xMotivo = buscar_valor_xpath(protNFe, 'xMotivo') if protNFe is not None else ''

            for det in infNFe.findall('nfe:det', NS):
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
            print(f"Erro no arquivo {caminho}: {e}")

    if not dados:
        shutil.rmtree(session_folder, ignore_errors=True)
        raise HTTPException(status_code=400, detail="Nenhum dado encontrado após aplicar os filtros.")

    df = pd.DataFrame(dados)
    relatorio_path = os.path.join(session_folder, f"relatorio_{session_id}.xlsx")
    df.to_excel(relatorio_path, index=False)

    return FileResponse(relatorio_path, filename="relatorio_nfe.xlsx")

