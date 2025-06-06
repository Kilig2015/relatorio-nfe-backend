from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import io
import os
import uuid
import zipfile
import tempfile
from datetime import datetime

app = FastAPI()

# Liberando apenas o domínio da Vercel
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kxml.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

TEMP_DIR = "relatorios"
os.makedirs(TEMP_DIR, exist_ok=True)


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


def processar_xmls(xml_files, modo_linha_individual, dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto):
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

    for file_path in xml_files:
        try:
            tree = ET.parse(file_path)
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
                if not modo_linha_individual:
                    break

        except Exception as e:
            print(f"Erro ao processar XML: {e}")

    df = pd.DataFrame(dados)
    output_path = os.path.join(TEMP_DIR, f"relatorio_{uuid.uuid4().hex}.xlsx")
    df.to_excel(output_path, index=False)
    return output_path


@app.post("/gerar-relatorio")
async def gerar_relatorio(
    background_tasks: BackgroundTasks,
    xmls: List[UploadFile] = File(...),
    modo_linha_individual: bool = Form(False),
    dataInicio: str = Form(None),
    dataFim: str = Form(None),
    cfop: str = Form(None),
    tipoNF: str = Form(None),
    ncm: str = Form(None),
    codigoProduto: str = Form(None)
):
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_paths = []

        for upload in xmls:
            filename = upload.filename.lower()
            filepath = os.path.join(tmpdir, filename)
            with open(filepath, "wb") as f:
                f.write(await upload.read())

            if filename.endswith(".zip"):
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)
                for root_dir, _, files in os.walk(tmpdir):
                    for name in files:
                        if name.endswith(".xml"):
                            xml_paths.append(os.path.join(root_dir, name))
            elif filename.endswith(".xml"):
                xml_paths.append(filepath)

        if not xml_paths:
            return JSONResponse(status_code=400, content={"detail": "Nenhum XML válido encontrado."})

        relatorio_path = processar_xmls(
            xml_paths,
            modo_linha_individual,
            dataInicio,
            dataFim,
            cfop,
            tipoNF,
            ncm,
            codigoProduto
        )

    if not os.path.exists(relatorio_path) or os.path.getsize(relatorio_path) == 0:
        return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

    return FileResponse(
        relatorio_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(relatorio_path)
    )
