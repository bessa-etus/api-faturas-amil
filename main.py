import re, io
import pandas as pd
import PyPDF2
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

def parse_decimal_br(s):
    if s is None: return 0.0
    t = str(s).strip()
    neg = False
    if t.endswith('-'):
        neg = True
        t = t[:-1].strip()
    if t.startswith('-'):
        neg = True
        t = t[1:].strip()
    t = t.replace('.', '').replace(',', '.')
    try: v = float(t)
    except: v = 0.0
    return -v if neg else v

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def pdf_to_lines(pdf_bytes: bytes) -> list[str]:
    reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    text = "\n".join([(p.extract_text() or "") for p in reader.pages])
    lines =[clean_spaces(l) for l in text.splitlines()]
    return [l for l in lines if l]

RE_HDR = re.compile(r"^Contrato\s+(\d{5,})\s*[-–—]\s*(.+)$", re.IGNORECASE)
RE_FOOTER_START = re.compile(r"^(Sub\s*Total|Subtotal\s*1|Subtotal\s*2|Total\s+Geral)\b", re.IGNORECASE)
STOP_WORDS = re.compile(r"(Mensalidade\b|Unidade\b|Operadora\b|Filial\b|N\.\s*Fiscal\b|Emiss[aã]o\b|Vencimento\b)", re.IGNORECASE)
RE_NF = re.compile(r"N\.\s*Fiscal\s+(\d+)", re.IGNORECASE)
RE_PLANO = re.compile(r"(AMIL\s+ONE\s+[A-Z0-9\s\-]+)", re.IGNORECASE)
RE_DEP_IN_TOTAL = re.compile(r"Valor\s*:\s*Total\s+Contrato\b.*?\bDependentes\s*:\s*(\d+)", re.IGNORECASE)
RE_MONEY_TOTAL = r"([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2}-?)"
RE_TOTAL_FMT1 = re.compile(r"Total\s+Contrato\b.*?\bValor\s*:\s*" + RE_MONEY_TOTAL, re.IGNORECASE)
RE_TOTAL_FMT2 = re.compile(RE_MONEY_TOTAL + r"\s+Valor\s*:\s*Total\s+Contrato\b", re.IGNORECASE)
RE_DEV_STRICT = re.compile(r"\b(Devolu[cç][aã]o|Cr[ée]dito|Ajuste)\b", re.IGNORECASE)
RE_MONEY_ANY = re.compile(r"(\d{1,3}(?:\.\d{3})*,\d{2}-?|\-\d{1,3}(?:\.\d{3})*,\d{2})")

def split_segments(lines: list[str]) -> list[dict]:
    starts =[]
    for i, ln in enumerate(lines):
        m = RE_HDR.match(ln)
        if m: starts.append((i, m.group(1), clean_spaces(m.group(2))))

    segments =[]
    for idx, (i, contrato, empresa) in enumerate(starts):
        j = starts[idx+1][0] if idx+1 < len(starts) else len(lines)
        seg_lines = lines[i:j]
        cut_pos = None
        for k, ln in enumerate(seg_lines):
            if RE_FOOTER_START.search(ln):
                cut_pos = k
                break
        if cut_pos is not None and cut_pos > 0:
            seg_lines = seg_lines[:cut_pos]
        segments.append({"contrato": contrato, "empresa_header": empresa, "lines": seg_lines})
    return segments

def clean_empresa(raw: str) -> str:
    raw = clean_spaces(raw)
    parts = STOP_WORDS.split(raw, maxsplit=1)
    base = clean_spaces(parts[0])
    base = re.sub(r"(RIO DE JANEIRO\s*){2,}$", "RIO DE JANEIRO", base, flags=re.IGNORECASE).strip()
    return base

def extract_nf(seg_lines: list[str]) -> str:
    for ln in seg_lines[:60]:
        m = RE_NF.search(ln)
        if m: return m.group(1)
    return ""

def extract_plano(seg_lines: list[str]) -> str:
    for ln in seg_lines:
        m = RE_PLANO.search(ln)
        if m:
            txt = clean_spaces(m.group(1))
            txt = re.split(r"\s+\d{1,3}(?:\.\d{3})*,\d{2}", txt)[0]
            return clean_spaces(txt)
    joined = " ".join(seg_lines).upper()
    if "DENTAL" in joined: return "DENTAL"
    if "AMIL" in joined: return "AMIL"
    return ""

def extract_dependentes(seg_lines: list[str]):
    for ln in seg_lines:
        m = RE_DEP_IN_TOTAL.search(ln)
        if m: return int(m.group(1))
    return ""

def extract_total(seg_lines: list[str]) -> float:
    for ln in seg_lines:
        m1 = RE_TOTAL_FMT1.search(ln)
        if m1: return parse_decimal_br(m1.group(1))
        m2 = RE_TOTAL_FMT2.search(ln)
        if m2: return parse_decimal_br(m2.group(1))
    return 0.0

def extract_devolucoes(seg_lines: list[str]) -> float:
    soma = 0.0
    for ln in seg_lines:
        if not RE_DEV_STRICT.search(ln): continue
        vals = RE_MONEY_ANY.findall(ln)
        for v in vals:
            num = parse_decimal_br(v)
            if num < 0: soma += num
    return round(soma, 2)

def build_df_from_segments(segments: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame({
        "contrato": [s["contrato"] for s in segments],
        "empresa": [clean_empresa(s["empresa_header"]) for s in segments],
        "nota fiscal": [extract_nf(s["lines"]) for s in segments],
        "nome do plano":[extract_plano(s["lines"]) for s in segments],
        "numero de dependentes":[extract_dependentes(s["lines"]) for s in segments],
        "valor total do contrato":[extract_total(s["lines"]) for s in segments],
        "devoluções":[extract_devolucoes(s["lines"]) for s in segments],
    })
    df["valor total do contrato"] = pd.to_numeric(df["valor total do contrato"], errors="coerce").fillna(0.0)
    df["devoluções"] = pd.to_numeric(df["devoluções"], errors="coerce").fillna(0.0)
    total_valor = float(df["valor total do contrato"].sum())
    total_dev   = float(df["devoluções"].sum())
    df_total = pd.DataFrame([{
        "contrato": "TOTAL FATURA", "empresa": "", "nota fiscal": "", "nome do plano": "",
        "numero de dependentes": "", "valor total do contrato": total_valor, "devoluções": total_dev,
    }])
    return pd.concat([df, df_total], ignore_index=True)

def process_etus_media(segments: list[dict]) -> pd.DataFrame:
    try:
        etus_lines =[]
        for seg in segments:
            if "ETUS MEDIA" in seg["empresa_header"].upper():
                etus_lines.extend(seg["lines"])

        if not etus_lines:
            return pd.DataFrame([{"Empresa": "ETUS MEDIA HOLDING", "Matr Funcional": "-", "NOME DO TITULAR": "Contrato ETUS não encontrado", "Valor Total da Matrícula": 0.0}])

        re_beneficiario = re.compile(r"(\d{9,15})\s+([A-Za-zÀ-ÿ\s]+?)\s+(\d{1,15})\s+(\d{11})\b")
        data_rows =[]

        for ln in etus_lines:
            m = re_beneficiario.search(ln)
            if m:
                nome = m.group(2).strip()
                matricula = m.group(3).strip()
                is_titular = bool(re.search(r"\bT\b", ln) or "Titular" in ln)
                money_matches = RE_MONEY_ANY.findall(ln)
                valor_unitario = 0.0
                if money_matches:
                    valor_unitario = parse_decimal_br(money_matches[0])

                data_rows.append({
                    "matricula": matricula,
                    "nome": nome,
                    "is_titular": is_titular,
                    "valor": valor_unitario
                })

        grouped = {}
        for row in data_rows:
            mat = row["matricula"]
            if mat not in grouped:
                grouped[mat] = {"titular_nome": row["nome"], "valor_total": 0.0, "achou_titular": False}
            if row["is_titular"]:
                grouped[mat]["titular_nome"] = row["nome"]
                grouped[mat]["achou_titular"] = True
            grouped[mat]["valor_total"] += row["valor"]

        output_data =[]
        for mat, info in grouped.items():
            output_data.append({
                "Empresa": "ETUS MEDIA HOLDING",
                "Matr Funcional": mat,
                "NOME DO TITULAR": info["titular_nome"],
                "Valor Total da Matrícula": round(info["valor_total"], 2)
            })

        if not output_data:
            return pd.DataFrame([{"Empresa": "ETUS MEDIA HOLDING", "Matr Funcional": "-", "NOME DO TITULAR": "Nenhum beneficiario detectado", "Valor Total da Matrícula": 0.0}])

        return pd.DataFrame(output_data)
    except Exception as e:
        return pd.DataFrame([{"Empresa": "ERRO LEITURA ETUS", "Matr Funcional": "-", "NOME DO TITULAR": str(e), "Valor Total da Matrícula": 0.0}])

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health(): return {"status": "A API da Amil esta online!"}

@app.post("/convert")
async def convert(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        lines = pdf_to_lines(pdf_bytes)
        segments = split_segments(lines)
        
        df_main = build_df_from_segments(segments)
        df_etus = process_etus_media(segments)

        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            df_main.to_excel(writer, index=False, sheet_name="Faturamento")
            df_etus.to_excel(writer, index=False, sheet_name="ETUS MEDIA")
            
        out_xlsx.seek(0)
        return StreamingResponse(
            out_xlsx,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=resultado_ATUALIZADO.xlsx"},
        )
    except Exception as e:
        return JSONResponse(content={"erro_geral": str(e)}, status_code=500)
