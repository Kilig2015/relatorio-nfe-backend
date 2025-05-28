from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse
import io
import pandas as pd

app = FastAPI()

@app.post('/gerar-relatorio')
def gerar_relatorio(xmls: list[UploadFile] = File(...), modo_linha_individual: bool = Form(...)):
    # Aqui usaria a lógica final para gerar o DataFrame a partir dos XMLs
    df = pd.DataFrame([{'refNFe': '123', 'produto': 'Produto Exemplo'}])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={"Content-Disposition": "attachment; filename=relatorio.xlsx"})