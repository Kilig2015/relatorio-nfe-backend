from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import io
from datetime import datetime
import zipfile

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

# Liberar acesso para qualquer frontend (pode restringir depois)
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
    dataInicio: str = Form(None),
    dataFim: str = Form(None),
    cfop: str = Form(None),
    tipoNF: str = Form(None),
    ncm: str = Form(None),
    codigoProduto: str = Form(None)
):
    # Limpeza dos filtros inválidos
    dataInicio = None if dataInicio in (None, '', 'string') else dataInicio
    dataFim = None if dataFim in (None, '', 'string') else dataFim
    cfop = None if cfop in (None, '', 'string') else cfop
    tipoNF = None if tipoNF in (None, '', 'string') else tipoNF
    ncm = None if ncm in (None, '', 'string') else ncm
    codigoProduto = None if codigoProduto in (None, '', 'string') else codigoProduto

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

    arquivos_processados = []

    for arquivo in xmls:
        if arquivo.filename.lower().endswith('.zip'):
            with zipfile.ZipFile(arquivo.file) as z:
                for nome_arquivo in z.namelist():
                    if nome_arquivo.lower().endswith('.xml'):
                        with z.open(nome_arquivo) as f:
                            arquivos_processados.append(f.read())
        elif arquivo.filename.lower().endswith('.xml'):
            arquivos_processados.append(await arquivo.read())

    dados = []

    for xml_bytes in arquivos_processados:
        try:
            root = ET.fromstring(xml_bytes)
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
