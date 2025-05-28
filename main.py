
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse
import pandas as pd
from io import BytesIO

app = FastAPI()

@app.post("/gerar-relatorio")
async def gerar_relatorio(xmls: list[UploadFile] = File(...), modo_linha_individual: bool = Form(...)):
    # Simulação de leitura de XMLs
    data = []
    for xml in xmls:
        data.append({
            "refNFe": "123",
            "produto": "Produto Exemplo"
        })

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={"Content-Disposition": "attachment; filename=relatorio.xlsx"})
