from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse
import pandas as pd
from io import BytesIO

app = FastAPI()

@app.post("/gerar-relatorio")
async def gerar_relatorio(xmls: list[UploadFile] = File(...), modo_linha_individual: bool = Form(...)):
    # Simulação da geração do DataFrame
    df = pd.DataFrame([{"refNFe": "123", "produto": "Produto Exemplo"}])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=relatorio.xlsx"})
