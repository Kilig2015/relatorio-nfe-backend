from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List
import zipfile, tempfile, io, os
import xml.etree.ElementTree as ET
import pandas as pd

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especifique o domínio
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
                resultado = subtag.find(f'nfe:{partes[-1]}', NS)
                if resultado is not None:
                    return resultado.text
            return ''
        atual = atual.find(f'nfe:{parte}', NS)
    return atual.text if atual is not None else ''

def processar_xml(conteudo_xml, filtros, modo_individual):
    tree = ET.parse(io.BytesIO(conteudo_xml))
    root = tree.getroot()
    infNFe = root.find('.//nfe:infNFe', NS)
    protNFe = root.find('.//nfe:protNFe/nfe:infProt', NS)
    chNFe = infNFe.attrib.get('Id', '').replace('NFe', '')
    xMotivo = buscar_valor_xpath(protNFe, 'xMotivo') if protNFe is not None else ''

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

        # Aplicar filtros (opcional)
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
        if not modo_individual:
            break
    return dados

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
    filtros = {
        'dataInicio': dataInicio,
        'dataFim': dataFim,
        'cfop': cfop,
        'tipoNF': tipoNF,
        'ncm': ncm,
        'codigoProduto': codigoProduto
    }

    todos_dados = []
    for upload in xmls:
        try:
            nome = upload.filename.lower()
            if nome.endswith('.zip'):
                with tempfile.TemporaryDirectory() as tmpdir:
                    path_zip = os.path.join(tmpdir, nome)
                    with open(path_zip, 'wb') as f:
                        f.write(await upload.read())

                    with zipfile.ZipFile(path_zip, 'r') as zip_ref:
                        for item in zip_ref.infolist():
                            if item.filename.endswith('.xml'):
                                with zip_ref.open(item) as xml_file:
                                    conteudo = xml_file.read()
                                    dados = processar_xml(conteudo, filtros, modo_linha_individual)
                                    todos_dados.extend(dados)
            else:
                conteudo = await upload.read()
                dados = processar_xml(conteudo, filtros, modo_linha_individual)
                todos_dados.extend(dados)
        except Exception as e:
            print(f"Erro ao processar {upload.filename}: {e}")

    if not todos_dados:
        return JSONResponse(status_code=400, content={"detail": "Nenhum dado encontrado após aplicar os filtros."})

    df = pd.DataFrame(todos_dados)
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=relatorio_nfe.xlsx"})
