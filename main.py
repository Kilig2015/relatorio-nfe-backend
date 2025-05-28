
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List
import shutil
import os

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
    with open("relatorio.xlsx", "wb") as buffer:
        buffer.write(b"DUMMY EXCEL FILE")
    return FileResponse("relatorio.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="relatorio.xlsx")
    