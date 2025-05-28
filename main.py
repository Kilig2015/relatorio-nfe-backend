
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List
import io
import openpyxl

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/gerar-relatorio")
async def gerar_relatorio(xmls: List[UploadFile] = File(...), modo_linha_individual: bool = Form(False)):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Relatório"

    ws.append(["refNFe", "produto"])
    for xml in xmls:
        ws.append(["123", "Produto Exemplo"])

    file_stream = io.BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    return StreamingResponse(file_stream, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": "attachment; filename=relatorio.xlsx"
    })
