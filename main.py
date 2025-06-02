from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from typing import List, Optional
import xml.etree.ElementTree as ET
import pandas as pd
import io, os, shutil, uuid, zipfile
from datetime import datetime
from pathlib import Path

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml-9g6sj9ab8-kiligs-projects-7cfc26f2.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
OUTPUT_DIR = "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def buscar_valor_xpath(base, caminho):
    partes = caminho.split('|')
    atual = base
    for parte in partes:
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
    dataInicio: Optional[str] = Form(None),
    dataFim: Optional[str] = Form(None),
    cfop: Optional[str] = Form(None),
    tipoNF: Optional[str] = Form(None),
    ncm: Optional[str] = Form(None),
    codigoProduto: Optional[str] = Form(None)
):
    task_id = str(uuid.uuid4())
    task_folder = Path(OUTPUT_DIR) / task_id
    task_folder.mkdir(parents=True, exist_ok=True)

    xml_paths = []

    for upload in xmls:
        if upload.filename.lower().endswith(".zip"):
            zip_path = task_folder / upload.filename
            with open(zip_path, "wb") as buffer:
                shutil.copyfileobj(upload.file, buffer)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(task_folder)
                for file in zip_ref.namelist():
                    if file.lower().endswith('.xml'):
                        xml_paths.append(task_folder / file)
        elif upload.filename.lower().endswith(".xml"):
            dest = task_folder / upload.filename
            with open(dest, "wb") as buffer:
                shutil.copyfileobj(upload.file, buffer)
            xml_paths.append(dest)

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

    for xml_path in xml_paths:
        try:
            tree = ET.parse(xml_path)
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
            print(f"Erro ao processar {xml_path.name}: {e}")

    if not dados:
        return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

    df = pd.DataFrame(dados)
    output_file = task_folder / "relatorio_nfe.xlsx"
    df.to_excel(output_file, index=False)

    return {"task_id": task_id}

@app.get("/status/{task_id}")
async def status(task_id: str):
    path = Path(OUTPUT_DIR) / task_id / "relatorio_nfe.xlsx"
    if path.exists():
        return {"status": "pronto"}
    return {"status": "processando"}

@app.get("/download/{task_id}")
async def download(task_id: str):
    path = Path(OUTPUT_DIR) / task_id / "relatorio_nfe.xlsx"
    if path.exists():
        return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            filename="relatorio_nfe.xlsx")
    return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
