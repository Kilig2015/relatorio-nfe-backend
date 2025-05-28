from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
from io import BytesIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/gerar-relatorio")
async def gerar_relatorio(xmls: list[UploadFile] = File(...), modo_linha_individual: bool = Form(...)):
    df = pd.DataFrame([{"refNFe": "123", "produto": "Produto Exemplo"}])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=relatorio.xlsx"}
    )
