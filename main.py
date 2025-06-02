from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from typing import List
import zipfile, tempfile, os, shutil, uuid
import xml.etree.ElementTree as ET
import pandas as pd
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especifique domínios permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

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

def processar_xmls(xml_paths, modo_linha_individual, filtros, output_path):
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
    for path in xml_paths:
        try:
            tree = ET.parse(path)
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
            continue

    df = pd.DataFrame(dados)
    df.to_excel(output_path, index=False)

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
    codigoProduto: str = Form(None),
):
    temp_dir = tempfile.mkdtemp()
    xml_paths = []

    for file in xmls:
        if file.filename.lower().endswith('.zip'):
            with zipfile.ZipFile(file.file) as zipf:
                zipf.extractall(temp_dir)
                for root_dir, _, files in os.walk(temp_dir):
                    for f in files:
                        if f.lower().endswith('.xml'):
                            xml_paths.append(os.path.join(root_dir, f))
        elif file.filename.lower().endswith('.xml'):
            temp_path = os.path.join(temp_dir, file.filename)
            with open(temp_path, 'wb') as out_file:
                content = await file.read()
                out_file.write(content)
            xml_paths.append(temp_path)

    if not xml_paths:
        shutil.rmtree(temp_dir)
        return JSONResponse(status_code=400, content={"detail": "Nenhum XML encontrado."})

    output_filename = f"relatorio_{uuid.uuid4().hex}.xlsx"
    output_path = os.path.join(temp_dir, output_filename)

    filtros = {
        'dataInicio': dataInicio,
        'dataFim': dataFim,
        'cfop': cfop,
        'tipoNF': tipoNF,
        'ncm': ncm,
        'codigoProduto': codigoProduto
    }

    background_tasks.add_task(processar_xmls, xml_paths, modo_linha_individual, filtros, output_path)
    return JSONResponse({"url": f"/download/{output_filename}"})


@app.get("/download/{filename}")
def baixar_arquivo(filename: str):
    caminho = os.path.join(tempfile.gettempdir(), filename)
    if not os.path.exists(caminho):
        return JSONResponse(status_code=404, content={"detail": "Arquivo não encontrado."})
    return FileResponse(caminho, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=filename)
