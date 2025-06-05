from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional
import xml.etree.ElementTree as ET
import pandas as pd
import io, os, zipfile, shutil, uuid
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ajuste se necessário
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
TEMP_DIR = "relatorios_temp"
os.makedirs(TEMP_DIR, exist_ok=True)

status_map = {}

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

def processar_relatorio(xml_files, output_path, filtros, modo_individual):
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

    for file in xml_files:
        try:
            tree = ET.parse(file)
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
                if not modo_individual:
                    break

        except Exception as e:
            print(f"Erro ao processar XML: {e}")

    df = pd.DataFrame(dados)
    df.to_excel(output_path, index=False)

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
    uid = str(uuid.uuid4())
    pasta = os.path.join(TEMP_DIR, uid)
    os.makedirs(pasta, exist_ok=True)

    arquivos_extraidos = []

    for arquivo in xmls:
        nome = arquivo.filename
        conteudo = await arquivo.read()
        caminho = os.path.join(pasta, nome)
        with open(caminho, 'wb') as f:
            f.write(conteudo)

        if nome.endswith('.zip'):
            with zipfile.ZipFile(caminho, 'r') as zip_ref:
                zip_ref.extractall(pasta)
            os.remove(caminho)

    for raiz, _, arquivos in os.walk(pasta):
        for nome in arquivos:
            if nome.lower().endswith('.xml'):
                arquivos_extraidos.append(os.path.join(raiz, nome))

    output_path = os.path.join(TEMP_DIR, f"relatorio_{uid}.xlsx")

    filtros = {
        "dataInicio": dataInicio or "",
        "dataFim": dataFim or "",
        "cfop": cfop or "",
        "tipoNF": tipoNF or "",
        "ncm": ncm or "",
        "codigoProduto": codigoProduto or "",
    }

    status_map[uid] = "processando"
    background_tasks.add_task(processar_relatorio, arquivos_extraidos, output_path, filtros, modo_linha_individual)
    return {"id": uid}

@app.get("/status/{id}")
def verificar_status(id: str):
    if id not in status_map:
        return {"status": "inexistente"}
    path = os.path.join(TEMP_DIR, f"relatorio_{id}.xlsx")
    if os.path.exists(path):
        return {"status": "pronto", "url": f"/download/{id}"}
    return {"status": "processando"}

@app.get("/download/{id}")
def download_relatorio(id: str):
    path = os.path.join(TEMP_DIR, f"relatorio_{id}.xlsx")
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
    return FileResponse(path, filename="relatorio_nfe.xlsx")
