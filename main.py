from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional
import xml.etree.ElementTree as ET
import pandas as pd
import io
from datetime import datetime
import zipfile
import os
import tempfile

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

# Altere o domínio abaixo conforme o domínio de produção do frontend (Vercel)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, substitua por [ "https://seu-projeto.vercel.app" ]
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

    arquivos_xml = []

    try:
        for arquivo in xmls:
            if arquivo.filename.endswith('.zip'):
                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = os.path.join(tmpdir, arquivo.filename)
                    with open(zip_path, "wb") as f:
                        f.write(await arquivo.read())
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(tmpdir)
                        for root, _, files in os.walk(tmpdir):
                            for name in files:
                                if name.endswith('.xml'):
                                    arquivos_xml.append(os.path.join(root, name))
            elif arquivo.filename.endswith('.xml'):
                arquivos_xml.append(arquivo.file)

        if not arquivos_xml:
            return JSONResponse(status_code=400, content={"detail": "Nenhum XML encontrado."})

        for xml_item in arquivos_xml:
            try:
                if isinstance(xml_item, str):  # caminho
                    tree = ET.parse(xml_item)
                else:  # UploadFile
                    tree = ET.parse(xml_item)

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

                    # Filtros opcionais
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

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"}
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Erro inesperado: {str(e)}"})
