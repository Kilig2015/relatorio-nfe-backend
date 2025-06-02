from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional
import xml.etree.ElementTree as ET
import pandas as pd
import io, os, uuid, shutil, zipfile
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste para ['https://seu-dominio.vercel.app'] se quiser restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
OUTPUT_DIR = "relatorios"
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

def processar_xmls_e_salvar(xmls, filtros, modo_linha_individual, filename):
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

    for xml_path in xmls:
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

                # filtros
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
                if not modo_linha_individual:
                    break

        except Exception as e:
            print(f"Erro ao processar XML: {e}")

    if not dados:
        return False

    df = pd.DataFrame(dados)
    df.to_excel(filename, index=False)
    return True

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
    codigoProduto: Optional[str] = Form(None)
):
    filtros = {
        "dataInicio": dataInicio,
        "dataFim": dataFim,
        "cfop": cfop,
        "tipoNF": tipoNF,
        "ncm": ncm,
        "codigoProduto": codigoProduto,
    }

    temp_dir = f"temp_{uuid.uuid4().hex}"
    os.makedirs(temp_dir, exist_ok=True)
    xml_paths = []

    for file in xmls:
        if file.filename.lower().endswith('.zip'):
            zip_path = os.path.join(temp_dir, file.filename)
            with open(zip_path, 'wb') as f:
                f.write(await file.read())
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            for root_dir, _, files in os.walk(temp_dir):
                for name in files:
                    if name.lower().endswith(".xml"):
                        xml_paths.append(os.path.join(root_dir, name))
        elif file.filename.lower().endswith('.xml'):
            path = os.path.join(temp_dir, file.filename)
            with open(path, 'wb') as f:
                f.write(await file.read())
            xml_paths.append(path)

    if not xml_paths:
        shutil.rmtree(temp_dir)
        return JSONResponse(status_code=400, content={"detail": "Nenhum XML encontrado."})

    report_id = uuid.uuid4().hex
    output_path = os.path.join(OUTPUT_DIR, f"relatorio_{report_id}.xlsx")

    def processar():
        sucesso = processar_xmls_e_salvar(xml_paths, filtros, modo_linha_individual, output_path)
        shutil.rmtree(temp_dir)
        if not sucesso:
            if os.path.exists(output_path):
                os.remove(output_path)

    background_tasks.add_task(processar)

    return {"link": f"https://relatorio-nfe-backend.onrender.com/download/relatorio_{report_id}.xlsx"}

@app.get("/download/{filename}")
def baixar_arquivo(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
    return FileResponse(path, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=filename)
