from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import xml.etree.ElementTree as ET

app = FastAPI()

@app.post("/gerar-relatorio")
async def gerar_relatorio(xmls: list[UploadFile] = File(...), modo_linha_individual: bool = Form(...)):
    dados = []

    for xml_file in xmls:
        content = await xml_file.read()
        root = ET.fromstring(content)

        # Extração de exemplo
        refNFe = root.findtext(".//infNFe/ide/nNF")
        emitente = root.findtext(".//emit/xNome")
        data_emissao = root.findtext(".//ide/dhEmi")

        for det in root.findall(".//det"):
            prod = det.find("prod")
            produto = prod.findtext("xProd")
            quantidade = prod.findtext("qCom")
            valor_unit = prod.findtext("vUnCom")
            valor_total = prod.findtext("vProd")
            ncm = prod.findtext("NCM")

            linha = {
                "refNFe": refNFe,
                "emitente": emitente,
                "dataEmissao": data_emissao,
                "produto": produto,
                "quantidade": quantidade,
                "valorUnitario": valor_unit,
                "valorTotal": valor_total,
                "NCM": ncm
            }
            dados.append(linha)

            if not modo_linha_individual:
                break  # só pega o primeiro item

    df = pd.DataFrame(dados)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatório')

    output.seek(0)
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={"Content-Disposition": "attachment; filename=relatorio.xlsx"}
    )
