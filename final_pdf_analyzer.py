"""
main.py

Пайплайн:
1) Берёт все PDF из папки source.
2) Через MinerU парсит каждый в Markdown (<stem>.md).
3) Из .md вытаскивает значения по кодам строк отчётности.
4) Считает показатели финансовой устойчивости, динамику и интегральный индекс.
5) Сохраняет результат в results/<stem>.txt:
   - по каждой строке баланса/ОФР: код, расшифровка, текущий и прошлый период;
   - по каждому коэффициенту: имя, расшифровка, значение;
   - темпы прироста по ключевым строкам;
   - интегральный показатель устойчивости и оценки по каждому коэффициенту.
"""

from pathlib import Path
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup
from mineru.cli.common import do_parse, read_fn


# --------- пути ---------

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "source"
OUT_DIR = BASE_DIR / "new_out"
RESULTS_DIR = BASE_DIR / "results"

OUT_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# --------- словарь расшифровок кодов строк ---------

CODE_DESCRIPTIONS = {
    # баланс, актив
    "1100": "Итого внеоборотные активы (раздел I актива баланса)",
    "1150": "Основные средства",
    "1170": "Долгосрочные финансовые вложения",
    "1200": "Итого оборотные активы (раздел II актива баланса)",
    "1210": "Запасы",
    "1230": "Дебиторская задолженность",
    "1240": "Краткосрочные финансовые вложения",
    "1250": "Денежные средства и денежные эквиваленты",
    "1600": "Валюта баланса (итог актива)",
    # баланс, пассив
    "1300": "Итого капитал и резервы (собственный капитал)",
    "1400": "Итого долгосрочные обязательства (раздел IV пассива)",
    "1500": "Итого краткосрочные обязательства (раздел V пассива)",
    "1530": "Доходы будущих периодов (краткосрочные)",
    "1540": "Оценочные обязательства (краткосрочные)",
    "1550": "Прочие краткосрочные обязательства",
    "1700": "Баланс (итог пассива)",
    # отчёт о финансовых результатах
    "2110": "Выручка",
    "2120": "Себестоимость продаж",
    "2200": "Прибыль (убыток) от продаж",
    "2220": "Управленческие расходы",
    "2300": "Прибыль (убыток) до налогообложения",
    "2330": "Проценты к уплате",
    "2400": "Чистая прибыль (убыток) отчетного периода",
}

NEEDED_CODES = set(CODE_DESCRIPTIONS.keys())


# --------- расшифровки показателей ---------

RATIO_DESCRIPTIONS = {
    "currentratio": "Коэффициент текущей ликвидности (1200 / (1500 - 1530))",
    "quickratio": "Коэффициент быстрой ликвидности ((1200 - 1210) / 1500)",
    "koeffindep": "Коэффициент автономии (1300 / 1600)",
    "perccovratio": (
        "Доля активов, покрытых собственным капиталом и долгосрочными "
        "обязательствами ((1300 + 1400) / 1600)"
    ),
    "equityratio": (
        "Доля собственного капитала в устойчивых источниках (1300 / (1300 + 1400))"
    ),
    "finlevratio": "Коэффициент финансового левериджа ((1400 + 1500) / 1300)",
    "maneuvcoef": (
        "Коэффициент манёвренности собственного капитала ((1300 - 1100) / 1300)"
    ),
    "constassetratio": "Доля внеоборотных активов в валюте баланса (1100 / 1600)",
    "coefofownfunds": (
        "Коэффициент обеспеченности собственными оборотными средствами "
        "((1300 - 1100) / 1200)"
    ),
    "net_margin": "Чистая маржа (2400 / 2110)",
    "operating_margin": "Операционная маржа (2200 / 2110)",
    "roe_like": "Рентабельность активов/капитала (2400 / 1600, без усреднения)",
    "interest_coverage": "Покрытие процентов ((2300 + |2330|) / |2330|)",
    "normofprib": "Маржа чистой прибыли (2400 / 2110)",
}


# --------- утилиты ---------

def parse_number(x):
    """Преобразовать строку вида '1 234', '(5 678)', '' в float или None."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).replace(" ", "").replace("\u00a0", "")
    s = s.replace("(", "-").replace(")", "")
    s = s.replace(",", ".")
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def growth_rate(current, previous):
    """
    Темп прироста (current / previous - 1) в долях.
    Если previous == 0 или данные отсутствуют — возвращает None.
    """
    if current is None or previous is None:
        return None
    try:
        current = float(current)
        previous = float(previous)
    except (TypeError, ValueError):
        return None
    if previous == 0:
        return None
    return current / previous - 1.0


def run_mineru(pdf_path: Path, out_dir: Path):
    """Запустить MinerU для одного PDF."""
    do_parse(
        output_dir=str(out_dir),
        pdf_file_names=[pdf_path.stem],
        pdf_bytes_list=[read_fn(pdf_path)],
        p_lang_list=["ru"],
        backend="pipeline",
        parse_method="auto",
        formula_enable=True,
        table_enable=True,
    )


def find_md_path(pdf_stem: str, out_dir: Path) -> Path:
    """Найти <stem>.md, сгенерированный MinerU."""
    md_path_auto = out_dir / pdf_stem / "auto" / f"{pdf_stem}.md"
    md_path_plain = out_dir / pdf_stem / f"{pdf_stem}.md"

    if md_path_auto.is_file():
        return md_path_auto
    if md_path_plain.is_file():
        return md_path_plain
    raise FileNotFoundError(
        f"{pdf_stem}.md не найден ни в {md_path_auto}, ни в {md_path_plain}"
    )


def extract_codes_from_md(md_path: Path) -> dict:
    """
    Прочитать <stem>.md, найти все HTML-таблицы и собрать значения по нужным кодам.
    Возвращает словарь: code -> {'current': float|None, 'previous': float|None}.
    """
    text = md_path.read_text(encoding="utf-8")

    soup = BeautifulSoup(text, "html.parser")
    tables = soup.find_all("table")

    codes = {}

    for tbl in tables:
        try:
            df = pd.read_html(StringIO(str(tbl)))[0]
        except Exception:
            continue

        if df.shape[1] < 4:
            continue

        df.columns = [str(c).strip() for c in df.columns]

        # ищем колонку с заголовком "Код"
        code_col_name = None
        for col in df.columns:
            if "код" in col.lower():
                code_col_name = col
                break

        # если заголовок "Код" в первой строке таблицы
        if code_col_name is None:
            first_row = df.iloc[0]
            for col in df.columns:
                if "код" in str(first_row[col]).lower():
                    code_col_name = col
                    df = df.iloc[1:].reset_index(drop=True)
                    break

        if code_col_name is None:
            continue

        current_col_name = df.columns[-2]
        prev_col_name = df.columns[-1]

        for _, row in df.iterrows():
            code_val = row[code_col_name]
            if pd.isna(code_val):
                continue
            code_str = str(code_val).strip()
            if code_str not in NEEDED_CODES:
                continue

            current_val = parse_number(row[current_col_name])
            prev_val = parse_number(row[prev_col_name])

            if code_str not in codes:
                codes[code_str] = {"current": None, "previous": None}
            if current_val is not None:
                codes[code_str]["current"] = current_val
            if prev_val is not None:
                codes[code_str]["previous"] = prev_val

    return codes


def calc_financial_ratios_from_codes(codes: dict) -> dict:
    """
    Возвращает словарь:
      {
        "levels": {... коэффициенты по текущему году ...},
        "growth": {... темпы прироста по ключевым строкам ...}
      }
    """

    def v_cur(code: str):
        return codes.get(code, {}).get("current")

    def v_prev(code: str):
        return codes.get(code, {}).get("previous")

    # уровни по текущему году
    VA = v_cur("1100")
    OA = v_cur("1200")
    A = v_cur("1600")
    SK = v_cur("1300")
    DO = v_cur("1400")
    KO = v_cur("1500")
    DBP = v_cur("1530")
    V = v_cur("2110")
    Seb = v_cur("2120")
    Uprav = v_cur("2220")
    PrProd = v_cur("2200")
    PrDoNal = v_cur("2300")
    ProcKUp = v_cur("2330")
    ChP = v_cur("2400")
    Z = v_cur("1210")

    levels = {}

    # 1) Ликвидность
    if OA is not None and KO is not None and DBP is not None and (KO - DBP) != 0:
        levels["currentratio"] = OA / (KO - DBP)
    if OA is not None and Z is not None and KO not in (None, 0):
        levels["quickratio"] = (OA - Z) / KO

    # 2) Структура капитала
    if SK is not None and A not in (None, 0):
        levels["koeffindep"] = SK / A
    if SK is not None and DO is not None and A not in (None, 0):
        levels["perccovratio"] = (SK + DO) / A
    if SK is not None and DO is not None and (SK + DO) != 0:
        levels["equityratio"] = SK / (SK + DO)
    if SK not in (None, 0) and DO is not None and KO is not None:
        levels["finlevratio"] = (DO + KO) / SK

    # 3) Собственные оборотные средства и манёвренность
    if SK not in (None, 0) and VA is not None:
        levels["maneuvcoef"] = (SK - VA) / SK
    if VA is not None and A not in (None, 0):
        levels["constassetratio"] = VA / A
    if OA not in (None, 0) and SK is not None and VA is not None:
        levels["coefofownfunds"] = (SK - VA) / OA

    # 4) Рентабельность и маржа
    if V not in (None, 0) and ChP is not None:
        levels["net_margin"] = ChP / V
        levels["normofprib"] = ChP / V
    if V not in (None, 0) and PrProd is not None:
        levels["operating_margin"] = PrProd / V
    if A not in (None, 0) and ChP is not None:
        levels["roe_like"] = ChP / A

    # 5) Покрытие процентов
    if ProcKUp is not None and ProcKUp != 0 and PrDoNal is not None:
        levels["interest_coverage"] = (PrDoNal + abs(ProcKUp)) / abs(ProcKUp)

    # ---------- Динамика (темпы прироста) по ключевым строкам ----------

    growth = {}
    for code in sorted(NEEDED_CODES):
        cur = v_cur(code)
        prev = v_prev(code)
        gr = growth_rate(cur, prev)
        if gr is not None:
            growth[f"growth_{code}"] = gr  # например, growth_2110 = темп прироста выручки

    return {"levels": levels, "growth": growth}


# --------- интегральный индекс ---------

def _score_linear(x, xmin, xmax, reverse=False):
    """
    Линейная шкала 0-1.
    reverse=True — чем меньше x, тем лучше (для finlevratio, constassetratio и т.п.).
    """
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None

    if not reverse:
        if x <= xmin:
            return 0.0
        if x >= xmax:
            return 1.0
        return (x - xmin) / (xmax - xmin)
    else:
        if x <= xmin:
            return 1.0
        if x >= xmax:
            return 0.0
        return (xmax - x) / (xmax - xmin)


def calc_fsi_index(levels: dict) -> dict:
    """
    Интегральный показатель финансовой устойчивости (FSI) 0..1
    + поэлементные оценки коэффициентов.
    """
    scores = {}

    # currentratio: 1.0 при >= 2.5, 0 при <= 1.0
    scores["currentratio"] = _score_linear(
        levels.get("currentratio"), xmin=1.0, xmax=2.5, reverse=False
    )

    # quickratio: 1.0 при >= 1.5, 0 при <= 0.7
    scores["quickratio"] = _score_linear(
        levels.get("quickratio"), xmin=0.7, xmax=1.5, reverse=False
    )

    # koeffindep: 1.0 при >= 0.6, 0 при <= 0.3
    scores["koeffindep"] = _score_linear(
        levels.get("koeffindep"), xmin=0.3, xmax=0.6, reverse=False
    )

    # perccovratio: 1.0 при >= 0.9, 0 при <= 0.6
    scores["perccovratio"] = _score_linear(
        levels.get("perccovratio"), xmin=0.6, xmax=0.9, reverse=False
    )

    # equityratio: 1.0 при >= 0.7, 0 при <= 0.3
    scores["equityratio"] = _score_linear(
        levels.get("equityratio"), xmin=0.3, xmax=0.7, reverse=False
    )

    # finlevratio: чем меньше, тем лучше; 1.0 при <= 1.0, 0 при >= 3.0
    scores["finlevratio"] = _score_linear(
        levels.get("finlevratio"), xmin=1.0, xmax=3.0, reverse=True
    )

    # maneuvcoef: 1.0 при >= 0.3, 0 при <= 0.0
    scores["maneuvcoef"] = _score_linear(
        levels.get("maneuvcoef"), xmin=0.0, xmax=0.3, reverse=False
    )

    # constassetratio: чем меньше, тем лучше; 1.0 при <= 0.6, 0 при >= 0.9
    scores["constassetratio"] = _score_linear(
        levels.get("constassetratio"), xmin=0.6, xmax=0.9, reverse=True
    )

    # coefofownfunds: 1.0 при >= 0.3, 0 при <= 0.0
    scores["coefofownfunds"] = _score_linear(
        levels.get("coefofownfunds"), xmin=0.0, xmax=0.3, reverse=False
    )

    # net_margin: 1.0 при >= 0.2, 0 при <= 0.02
    scores["net_margin"] = _score_linear(
        levels.get("net_margin"), xmin=0.02, xmax=0.2, reverse=False
    )

    valid_scores = [v for v in scores.values() if v is not None]
    fsi = sum(valid_scores) / len(valid_scores) if valid_scores else None

    return {"scores": scores, "fsi": fsi}


# --------- вывод в txt ---------

def write_result_txt(
    pdf_path: Path, codes: dict, levels: dict, growth: dict, fsi_info: dict
):
    """
    Сохранить результат в results/<stem>.txt:
    - строки отчётности (код, описание, текущий, прошлый),
    - уровни коэффициентов,
    - динамика (темпы прироста по ключевым кодам),
    - интегральный индекс FSI и оценки коэффициентов.
    """
    lines = []

    lines.append(f"Файл PDF: {pdf_path.name}")
    lines.append("")

    lines.append("=== Строки отчётности (баланс и ОФР) ===")
    for code in sorted(NEEDED_CODES):
        desc = CODE_DESCRIPTIONS.get(code, "")
        vals = codes.get(code)
        cur = vals.get("current") if vals else None
        prev = vals.get("previous") if vals else None
        lines.append(
            f"Код {code}: {desc}\n"
            f"  Текущий период: {cur}\n"
            f"  Прошлый период: {prev}"
        )

    lines.append("")
    lines.append("=== Показатели финансовой устойчивости (уровни) ===")
    for name, value in levels.items():
        desc = RATIO_DESCRIPTIONS.get(name, "")
        lines.append(f"{name}: {value}  # {desc}")

    lines.append("")
    lines.append("=== Динамика ключевых показателей (темп прироста) ===")
    lines.append("Темп прироста рассчитывается как (текущий / прошлый - 1), в долях.")
    for key, val in growth.items():
        code = key.split("_", 1)[1]
        desc = CODE_DESCRIPTIONS.get(code, "")
        lines.append(f"{key}: {val}  # {code} — {desc}")

    lines.append("")
    lines.append("=== Интегральный показатель финансовой устойчивости ===")
    fsi = fsi_info.get("fsi")
    lines.append(f"FSI (0..1): {fsi}")
    lines.append("")
    lines.append("Оценки отдельных коэффициентов (0..1):")
    for name, score in fsi_info.get("scores", {}).items():
        desc = RATIO_DESCRIPTIONS.get(name, "")
        lines.append(f"{name}: {score}  # {desc}")

    txt_path = RESULTS_DIR / f"{pdf_path.stem}.txt"
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Результат сохранён в {txt_path}")


# --------- основной сценарий ---------

def process_pdf(pdf_path: Path):
    print(f"Обработка: {pdf_path.name}")

    run_mineru(pdf_path, OUT_DIR)
    md_path = find_md_path(pdf_path.stem, OUT_DIR)
    codes = extract_codes_from_md(md_path)
    ratios_all = calc_financial_ratios_from_codes(codes)
    levels = ratios_all["levels"]
    growth = ratios_all["growth"]
    fsi_info = calc_fsi_index(levels)
    write_result_txt(pdf_path, codes, levels, growth, fsi_info)


if __name__ == "__main__":
    pdf_files = sorted(SOURCE_DIR.glob("*.pdf"))
    if not pdf_files:
        print(f"В {SOURCE_DIR} нет файлов .pdf")
    else:
        for pdf in pdf_files:
            process_pdf(pdf)