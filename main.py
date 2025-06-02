from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from uuid import uuid4
import os, shutil, zipfile, xml.etree.ElementTree as ET, pandas as pd
from datetime import datetime

TEMP_FOLDER = "tarefas"
os.makedirs(TEMP_FOLDER, exist_ok=True)

NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

app = FastAPI()

# CORS para produção (ajuste seu domínio se quiser restringir)
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
    task_id = str(uuid4())
    task_folder = os.path.join(TEMP_FOLDER, task_id)
    os.makedirs(task_folder, exist_ok=True)

    for file in xmls:
        path = os.path.join(task_folder, file.filename)
        with open(path, "wb") as f:
            content = await file.read()
            f.write(content)

    # Descompactar arquivos zip
    for file in xmls:
        if file.filename.endswith(".zip"):
            try:
                zip_path = os.path.join(task_folder, file.filename)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(task_folder)
                os.remove(zip_path)
            except Exception as e:
                return JSONResponse(status_code=400, content={"detail": f"Erro ao extrair ZIP: {e}"})

    background_tasks.add_task(
        processar_tarefa,
        task_id, modo_linha_individual,
        dataInicio, dataFim, cfop, tipoNF, ncm, codigoProduto
    )
    return {"message": "Processamento iniciado", "task_id": task_id}

async def processar_tarefa(task_id, modo_individual, dataInicio, dataFim, cfop, tipoNF, ncm, cod_produto):
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

    task_folder = os.path.join(TEMP_FOLDER, task_id)
    arquivos = [os.path.join(task_folder, f) for f in os.listdir(task_folder) if f.endswith('.xml')]
    dados = []

    for path in arquivos:
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
                if cod_produto and linha["Código Produto"] != cod_produto:
                    continue

                dados.append(linha)
                if not modo_individual:
                    break
        except Exception as e:
            print(f"Erro ao processar {path}: {e}")

    output_path = os.path.join(task_folder, "relatorio_nfe.xlsx")
    if not dados:
        with open(output_path, "w") as f:
            f.write("Nenhum dado encontrado após aplicar os filtros.")
    else:
        df = pd.DataFrame(dados)
        df.to_excel(output_path, index=False)

@app.get("/status/{task_id}")
def verificar_status(task_id: str):
    task_folder = os.path.join(TEMP_FOLDER, task_id)
    file_path = os.path.join(task_folder, "relatorio_nfe.xlsx")
    if os.path.exists(file_path):
        return {"status": "pronto", "download_url": f"/download/{task_id}"}
    elif os.path.exists(task_folder):
        return {"status": "processando"}
    else:
        return JSONResponse(status_code=404, content={"detail": "ID não encontrado"})

@app.get("/download/{task_id}")
def baixar_arquivo(task_id: str):
    task_folder = os.path.join(TEMP_FOLDER, task_id)
    file_path = os.path.join(task_folder, "relatorio_nfe.xlsx")
    if not os.path.exists(file_path):
        return JSONResponse(status_code=404, content={"detail": "Arquivo ainda não gerado"})
    return FileResponse(file_path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="relatorio_nfe.xlsx")
