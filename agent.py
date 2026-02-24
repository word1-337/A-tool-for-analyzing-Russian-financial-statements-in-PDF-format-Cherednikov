"""
Агент анализа финансовой устойчивости.

Запуск:
    streamlit run agent.py
"""

from pathlib import Path
from io import StringIO

import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
from mineru.cli.common import do_parse, read_fn

try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False


# ══════════════════════════════════════════════════════════════
#  ПУТИ
# ══════════════════════════════════════════════════════════════

BASE_DIR    = Path(__file__).resolve().parent
SOURCE_DIR  = BASE_DIR / "source"
OUT_DIR     = BASE_DIR / "new_out"
RESULTS_DIR = BASE_DIR / "results"

SOURCE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  СЛОВАРИ
# ══════════════════════════════════════════════════════════════

CODE_DESCRIPTIONS = {
    "1100": "Итого внеоборотные активы (раздел I актива баланса)",
    "1150": "Основные средства",
    "1170": "Долгосрочные финансовые вложения",
    "1200": "Итого оборотные активы (раздел II актива баланса)",
    "1210": "Запасы",
    "1230": "Дебиторская задолженность",
    "1240": "Краткосрочные финансовые вложения",
    "1250": "Денежные средства и денежные эквиваленты",
    "1600": "Валюта баланса (итог актива)",
    "1300": "Итого капитал и резервы (собственный капитал)",
    "1400": "Итого долгосрочные обязательства (раздел IV пассива)",
    "1500": "Итого краткосрочные обязательства (раздел V пассива)",
    "1530": "Доходы будущих периодов (краткосрочные)",
    "1540": "Оценочные обязательства (краткосрочные)",
    "1550": "Прочие краткосрочные обязательства",
    "1700": "Баланс (итог пассива)",
    "2110": "Выручка",
    "2120": "Себестоимость продаж",
    "2200": "Прибыль (убыток) от продаж",
    "2220": "Управленческие расходы",
    "2300": "Прибыль (убыток) до налогообложения",
    "2330": "Проценты к уплате",
    "2400": "Чистая прибыль (убыток) отчетного периода",
}

NEEDED_CODES = set(CODE_DESCRIPTIONS.keys())

RATIO_DESCRIPTIONS = {
    "currentratio":    "Коэффициент текущей ликвидности (1200 / (1500 - 1530))",
    "quickratio":      "Коэффициент быстрой ликвидности ((1200 - 1210) / 1500)",
    "koeffindep":      "Коэффициент автономии (1300 / 1600)",
    "perccovratio":    "Доля активов, покрытых СК и ДО ((1300 + 1400) / 1600)",
    "equityratio":     "Доля СК в устойчивых источниках (1300 / (1300 + 1400))",
    "finlevratio":     "Коэффициент финансового левериджа ((1400 + 1500) / 1300)",
    "maneuvcoef":      "Коэффициент манёвренности СК ((1300 - 1100) / 1300)",
    "constassetratio": "Доля внеоборотных активов в валюте баланса (1100 / 1600)",
    "coefofownfunds":  "Коэффициент обеспеченности СОС ((1300 - 1100) / 1200)",
    "net_margin":      "Чистая маржа (2400 / 2110)",
    "operating_margin":"Операционная маржа (2200 / 2110)",
    "roe_like":        "Рентабельность активов (2400 / 1600)",
    "interest_coverage":"Покрытие процентов ((2300 + |2330|) / |2330|)",
    "normofprib":      "Маржа чистой прибыли (2400 / 2110)",
}


# ══════════════════════════════════════════════════════════════
#  BACKEND — утилиты и расчёты
# ══════════════════════════════════════════════════════════════

def parse_number(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).replace(" ", "").replace("\u00a0", "").replace("(", "-").replace(")", "").replace(",", ".")
    if s in ("", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def growth_rate(current, previous):
    if current is None or previous is None:
        return None
    try:
        current, previous = float(current), float(previous)
    except (TypeError, ValueError):
        return None
    return None if previous == 0 else current / previous - 1.0


def run_mineru(pdf_path: Path):
    do_parse(
        output_dir=str(OUT_DIR),
        pdf_file_names=[pdf_path.stem],
        pdf_bytes_list=[read_fn(pdf_path)],
        p_lang_list=["ru"],
        backend="pipeline",
        parse_method="auto",
        formula_enable=True,
        table_enable=True,
    )


def find_md_path(pdf_stem: str) -> Path:
    for candidate in [
        OUT_DIR / pdf_stem / "auto" / f"{pdf_stem}.md",
        OUT_DIR / pdf_stem / f"{pdf_stem}.md",
    ]:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Файл {pdf_stem}.md не найден в {OUT_DIR}")


def extract_codes(md_path: Path) -> dict:
    soup = BeautifulSoup(md_path.read_text(encoding="utf-8"), "html.parser")
    codes = {}

    for tbl in soup.find_all("table"):
        try:
            df = pd.read_html(StringIO(str(tbl)))[0]
        except Exception:
            continue
        if df.shape[1] < 4:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        code_col = next((c for c in df.columns if "код" in c.lower()), None)

        if code_col is None:
            first = df.iloc[0]
            for c in df.columns:
                if "код" in str(first[c]).lower():
                    code_col = c
                    df = df.iloc[1:].reset_index(drop=True)
                    break
        if code_col is None:
            continue

        cur_col, prev_col = df.columns[-2], df.columns[-1]
        for _, row in df.iterrows():
            code_val = row[code_col]
            if pd.isna(code_val):
                continue
            code_str = str(code_val).strip()
            if code_str not in NEEDED_CODES:
                continue
            codes.setdefault(code_str, {"current": None, "previous": None})
            cur = parse_number(row[cur_col])
            prv = parse_number(row[prev_col])
            if cur is not None:
                codes[code_str]["current"] = cur
            if prv is not None:
                codes[code_str]["previous"] = prv

    return codes


def calc_ratios(codes: dict) -> dict:
    def c(code): return codes.get(code, {}).get("current")

    VA, OA, A   = c("1100"), c("1200"), c("1600")
    SK, DO, KO  = c("1300"), c("1400"), c("1500")
    DBP         = c("1530")
    V, PrProd   = c("2110"), c("2200")
    PrDoNal     = c("2300")
    ProcKUp     = c("2330")
    ChP, Z      = c("2400"), c("1210")

    lv = {}
    if OA is not None and KO is not None and DBP is not None and (KO - DBP) != 0:
        lv["currentratio"] = OA / (KO - DBP)
    if OA is not None and Z is not None and KO not in (None, 0):
        lv["quickratio"] = (OA - Z) / KO
    if SK is not None and A not in (None, 0):
        lv["koeffindep"] = SK / A
    if SK is not None and DO is not None and A not in (None, 0):
        lv["perccovratio"] = (SK + DO) / A
    if SK is not None and DO is not None and (SK + DO) != 0:
        lv["equityratio"] = SK / (SK + DO)
    if SK not in (None, 0) and DO is not None and KO is not None:
        lv["finlevratio"] = (DO + KO) / SK
    if SK not in (None, 0) and VA is not None:
        lv["maneuvcoef"] = (SK - VA) / SK
    if VA is not None and A not in (None, 0):
        lv["constassetratio"] = VA / A
    if OA not in (None, 0) and SK is not None and VA is not None:
        lv["coefofownfunds"] = (SK - VA) / OA
    if V not in (None, 0) and ChP is not None:
        lv["net_margin"] = lv["normofprib"] = ChP / V
    if V not in (None, 0) and PrProd is not None:
        lv["operating_margin"] = PrProd / V
    if A not in (None, 0) and ChP is not None:
        lv["roe_like"] = ChP / A
    if ProcKUp not in (None, 0) and PrDoNal is not None:
        lv["interest_coverage"] = (PrDoNal + abs(ProcKUp)) / abs(ProcKUp)

    growth = {}
    for code in sorted(NEEDED_CODES):
        gr = growth_rate(
            codes.get(code, {}).get("current"),
            codes.get(code, {}).get("previous"),
        )
        if gr is not None:
            growth[f"growth_{code}"] = gr

    return {"levels": lv, "growth": growth}


def _score(x, xmin, xmax, reverse=False):
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    if not reverse:
        return 0.0 if x <= xmin else (1.0 if x >= xmax else (x - xmin) / (xmax - xmin))
    else:
        return 1.0 if x <= xmin else (0.0 if x >= xmax else (xmax - x) / (xmax - xmin))


def calc_fsi(levels: dict) -> dict:
    scores = {
        "currentratio":    _score(levels.get("currentratio"),    1.0, 2.5),
        "quickratio":      _score(levels.get("quickratio"),      0.7, 1.5),
        "koeffindep":      _score(levels.get("koeffindep"),      0.3, 0.6),
        "perccovratio":    _score(levels.get("perccovratio"),    0.6, 0.9),
        "equityratio":     _score(levels.get("equityratio"),     0.3, 0.7),
        "finlevratio":     _score(levels.get("finlevratio"),     1.0, 3.0, reverse=True),
        "maneuvcoef":      _score(levels.get("maneuvcoef"),      0.0, 0.3),
        "constassetratio": _score(levels.get("constassetratio"), 0.6, 0.9, reverse=True),
        "coefofownfunds":  _score(levels.get("coefofownfunds"),  0.0, 0.3),
        "net_margin":      _score(levels.get("net_margin"),      0.02, 0.2),
    }
    valid = [v for v in scores.values() if v is not None]
    return {"scores": scores, "fsi": sum(valid) / len(valid) if valid else None}


def build_report(pdf_path: Path, codes: dict, levels: dict, growth: dict, fsi_info: dict) -> str:
    lines = [f"Файл PDF: {pdf_path.name}", ""]

    lines.append("=== Строки отчётности (баланс и ОФР) ===")
    for code in sorted(NEEDED_CODES):
        vals = codes.get(code)
        cur  = vals.get("current")  if vals else None
        prev = vals.get("previous") if vals else None
        lines.append(
            f"Код {code}: {CODE_DESCRIPTIONS.get(code, '')}\n"
            f"  Текущий период: {cur}\n"
            f"  Прошлый период: {prev}"
        )

    lines += ["", "=== Показатели финансовой устойчивости ==="]
    for name, value in levels.items():
        lines.append(f"{name}: {value}  # {RATIO_DESCRIPTIONS.get(name, '')}")

    lines += ["", "=== Динамика (темп прироста, в долях) ==="]
    for key, val in growth.items():
        code = key.split("_", 1)[1]
        lines.append(f"{key}: {val}  # {CODE_DESCRIPTIONS.get(code, '')}")

    lines += ["", "=== Интегральный показатель FSI (0..1) ==="]
    lines.append(f"FSI: {fsi_info.get('fsi')}")
    lines += ["", "Оценки коэффициентов:"]
    for name, score in fsi_info.get("scores", {}).items():
        lines.append(f"{name}: {score}  # {RATIO_DESCRIPTIONS.get(name, '')}")

    return "\n".join(lines)


def process_pdf(pdf_path: Path) -> str:
    run_mineru(pdf_path)
    md_path  = find_md_path(pdf_path.stem)
    codes    = extract_codes(md_path)
    ratios   = calc_ratios(codes)
    fsi_info = calc_fsi(ratios["levels"])
    report   = build_report(pdf_path, codes, ratios["levels"], ratios["growth"], fsi_info)

    txt_path = RESULTS_DIR / f"{pdf_path.stem}.txt"
    txt_path.write_text(report, encoding="utf-8")
    return report


# ══════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ══════════════════════════════════════════════════════════════

st.set_page_config(page_title="Агент финансовой устойчивости", layout="wide")
st.title("Агент анализа финансовой устойчивости")
st.markdown(
    "Загрузи PDF с российской бухгалтерской отчётностью (баланс, ОФР, ОДДС). "
    "Результат анализа сохраняется в папку `results/`."
)

uploaded = st.file_uploader("Выбери файл PDF", type=["pdf"])

if uploaded is not None:
    pdf_path = SOURCE_DIR / uploaded.name
    pdf_path.write_bytes(uploaded.getbuffer())
    st.success(f"Файл сохранён: {pdf_path}")

    if st.button("Проанализировать"):
        with st.spinner("Запускаю MinerU и считаю показатели… (до 5 минут)"):
            report = process_pdf(pdf_path)
        st.session_state["report"] = report
        st.session_state["stem"]   = pdf_path.stem

if "report" in st.session_state:
    report = st.session_state["report"]
    stem   = st.session_state["stem"]

    st.subheader("Отчёт по финансовой устойчивости")
    st.text_area("", value=report, height=400)
    st.download_button(
        "Скачать отчёт (.txt)",
        data=report.encode("utf-8"),
        file_name=f"{stem}.txt",
        mime="text/plain",
    )

    if not OLLAMA_AVAILABLE:
        st.info("Ollama не установлена — ИИ-комментарии недоступны. Выполни: `pip install ollama`")
    else:
        st.session_state.setdefault("ollama_summary", "")
        st.session_state.setdefault("ollama_answer", "")

        st.markdown("---")
        st.subheader("Краткий вывод ИИ (Ollama)")

        if st.button("Сформировать краткий вывод"):
            try:
                with st.spinner("Генерирую вывод…"):
                    resp = ollama.chat(
                        model="qwen2.5:32b",
                        messages=[
                            {
                                "role": "system",
                                "content": "Ты финансовый аналитик. Отвечай кратко, структурировано, на русском языке.",
                            },
                            {
                                "role": "user",
                                "content": (
                                    "Сделай краткий вывод о финансовой устойчивости компании: "
                                    "оцени уровень риска (низкий/средний/высокий), укажи сильные и слабые стороны.\n\n"
                                    + report
                                ),
                            },
                        ],
                    )
                    st.session_state["ollama_summary"] = resp["message"]["content"]
            except Exception as e:
                st.error(f"Ошибка Ollama: {e}")

        if st.session_state["ollama_summary"]:
            st.write(st.session_state["ollama_summary"])

        st.markdown("---")
        st.subheader("Задать вопрос ИИ по отчёту")

        question = st.text_input("Вопрос (например: «как изменилась выручка?»)", key="q")

        if st.button("Спросить") and question:
            try:
                with st.spinner("Отвечаю…"):
                    resp_q = ollama.chat(
                        model="qwen2.5:32b",
                        messages=[
                            {
                                "role": "system",
                                "content": "Ты финансовый аналитик. Отвечай на русском.",
                            },
                            {
                                "role": "user",
                                "content": f"ОТЧЁТ:\n{report}\n\nВОПРОС: {question}",
                            },
                        ],
                    )
                    st.session_state["ollama_answer"] = resp_q["message"]["content"]
            except Exception as e:
                st.error(f"Ошибка Ollama: {e}")

        if st.session_state["ollama_answer"]:
            st.write(st.session_state["ollama_answer"])
