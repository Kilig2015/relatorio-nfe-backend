from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import io

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

origins = [
    "https://kxml-x1d5zzmjb-kiligs-projects-7cfc26f2.vercel.app",  # seu domínio vercel
    "http://localhost:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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
    # Corrigir filtros 'string'
    filtros = {
        'dataInicio': dataInicio if dataInicio and dataInicio.lower() != "string" else None,
        'dataFim': dataFim if dataFim and dataFim.lower() != "string" else None,
        'cfop': cfop if cfop and cfop.lower() != "string" else None,
        'tipoNF': tipoNF if tipoNF and tipoNF.lower() != "string" else None,
        'ncm': ncm if ncm and ncm.lower() != "string" else None,
        'codigoProduto': codigoProduto if codigoProduto and codigoProduto.lower() != "string" else None
    }

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

                if filtros['dataInicio'] and data_emi < filtros['dataInicio']:
                    continue
                if filtros['dataFim'] and data_emi > filtros['dataFim']:
                    continue
                if filtros['cfop'] and linha["CFOP"] != filtros['cfop']:
                    continue
                if filtros['tipoNF'] and linha["Tipo NF"] != ("0" if filtros['tipoNF'] == "Entrada" else "1"):
                    continue
                if filtros['ncm'] and linha["NCM"] != filtros['ncm']:
                    continue
                if filtros['codigoProduto'] and linha["Código Produto"] != filtros['codigoProduto']:
                    continue

                dados.append(linha)
                if not modo_linha_individual:
                    break

        except Exception as e:
            print(f"Erro ao processar XML: {e}")
            continue

    if not dados:
        raise HTTPException(status_code=400, detail="Nenhum dado encontrado após aplicar os filtros.")

    df = pd.DataFrame(dados)
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"}
    )
