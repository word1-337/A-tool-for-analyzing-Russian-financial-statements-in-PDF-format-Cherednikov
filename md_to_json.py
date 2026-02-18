from mineru.cli.common import do_parse, read_fn  # структура модулей может меняться с версиями
from pathlib import Path

input_path = Path("C:/Users/furfu/Downloads/NewMakerProject/afk.pdf")
output_dir = "C:/Users/furfu/Downloads/NewMakerProject/new_out"

# запускаем парсер (read_fn сам прочитает pdf в bytes)
do_parse(
    output_dir=output_dir,
    pdf_file_names=[input_path.stem],        # имя файла без расширения
    pdf_bytes_list=[read_fn(input_path)],    # список байтов
    p_lang_list=["ru"],                      # язык документа: "en", "ch", "ru" и т.д.
    backend="pipeline",                      # или другой доступный backend
    parse_method="auto",                     # "auto" / "txt" / "ocr"
    formula_enable=True,
    table_enable=True,
)

from pathlib import Path
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

base_dir = Path("C:/Users/furfu/Downloads/NewMakerProject")

md_path_auto = base_dir / "new_out" / "afk" / "auto" / "afk.md"
md_path_plain = base_dir / "new_out" / "afk" / "afk.md"

if md_path_auto.is_file():
    md_path = md_path_auto
elif md_path_plain.is_file():
    md_path = md_path_plain
else:
    raise FileNotFoundError("afk.md не найден ни в new_out/afk/auto, ни в new_out/afk")

text = md_path.read_text(encoding="utf-8")

needed_codes = {
    # баланс, актив
    "1100", "1150", "1170", "1200", "1210", "1230", "1240", "1250", "1600",
    # баланс, пассив
    "1300", "1400", "1500", "1530", "1540", "1550", "1700",
    # отчёт о фин. результатах
    "2110", "2120", "2220", "2200", "2300", "2330", "2400",
}


def parse_number(x):
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


soup = BeautifulSoup(text, "html.parser")
tables = soup.find_all("table")

codes = {}  # code -> {"current": ..., "previous": ...}

for tbl in tables:
    try:
        df = pd.read_html(StringIO(str(tbl)))[0]
    except Exception:
        continue

    if df.shape[1] < 4:
        continue

    # нормализуем имена колонок в строки
    df.columns = [str(c).strip() for c in df.columns]

    # 1) ищем колонку с названием "Код"
    code_col_name = None
    for col in df.columns:
        if "код" in col.lower():
            code_col_name = col
            break

    # 2) если не нашли по заголовку – пробуем по первой строке
    if code_col_name is None:
        first_row = df.iloc[0]
        for col in df.columns:
            if "код" in str(first_row[col]).lower():
                code_col_name = col
                # удаляем строку с заголовком как часть данных
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
        if code_str not in needed_codes:
            continue

        current_val = parse_number(row[current_col_name])
        prev_val = parse_number(row[prev_col_name])

        if code_str not in codes:
            codes[code_str] = {"current": None, "previous": None}
        if current_val is not None:
            codes[code_str]["current"] = current_val
        if prev_val is not None:
            codes[code_str]["previous"] = prev_val

print("Собраны коды:")
for c in sorted(needed_codes):
    print(c, "->", codes.get(c))


def calc_financial_ratios_from_codes(codes: dict):
    def v(code: str):
        return codes.get(code, {}).get("current")

    VA = v("1100")
    OA = v("1200")
    A  = v("1600")
    SK = v("1300")
    DO = v("1400")
    KO = v("1500")
    DBP = v("1530")
    V  = v("2110")
    Seb = v("2120")
    Uprav = v("2220")
    PrProd = v("2200")
    PrDoNal = v("2300")
    ProcKUp = v("2330")
    ChP = v("2400")

    ratios = {}

    # currentratio = 1200 / (1500 - 1530)
    if OA is not None and KO is not None and DBP is not None and (KO - DBP) != 0:
        ratios["currentratio"] = OA / (KO - DBP)

    # koeffindep = 1300 / 1600
    if SK is not None and A not in (None, 0):
        ratios["koeffindep"] = SK / A

    # perccovratio = (1300 + 1400) / 1600
    if SK is not None and DO is not None and A not in (None, 0):
        ratios["perccovratio"] = (SK + DO) / A

    # equityratio = 1300 / (1300 + 1400)
    if SK is not None and DO is not None and (SK + DO) != 0:
        ratios["equityratio"] = SK / (SK + DO)

    # finlevratio = (1400 + 1500) / 1300
    if SK not in (None, 0) and DO is not None and KO is not None:
        ratios["finlevratio"] = (DO + KO) / SK

    # maneuvcoef = (1300 - 1100) / 1300
    if SK not in (None, 0) and VA is not None:
        ratios["maneuvcoef"] = (SK - VA) / SK

    # constassetratio = 1100 / 1600
    if VA is not None and A not in (None, 0):
        ratios["constassetratio"] = VA / A

    # coefofownfunds = (1300 - 1100) / 1200
    if OA not in (None, 0) and SK is not None and VA is not None:
        ratios["coefofownfunds"] = (SK - VA) / OA

    # normofprib = 2400 / 2110 (маржа чистой прибыли)
    if V not in (None, 0) and ChP is not None:
        ratios["normofprib"] = ChP / V

    return ratios



ratios = calc_financial_ratios_from_codes(codes)
print(ratios)