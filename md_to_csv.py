from pathlib import Path
import pandas as pd
import markdown
from bs4 import BeautifulSoup

base_dir = Path("C:/Users/furfu/Downloads/NewMakerProject")

# пробуем auto, потом обычный
md_path_auto = base_dir / "new_out" / "afk" / "auto" / "afk.md"
md_path_plain = base_dir / "new_out" / "afk" / "afk.md"

if md_path_auto.is_file():
    md_path = md_path_auto
elif md_path_plain.is_file():
    md_path = md_path_plain
else:
    raise FileNotFoundError("afk.md не найден ни в new_out/afk/auto, ни в new_out/afk")

out_dir = base_dir / "tables_csv"
out_dir.mkdir(parents=True, exist_ok=True)

text = md_path.read_text(encoding="utf-8")

html = markdown.markdown(text, extensions=["tables"])
soup = BeautifulSoup(html, "html.parser")
html_tables = soup.find_all("table")

if not html_tables:
    print("В HTML нет <table> — покажи кусок afk.md с таблицей.")
else:
    balance_df = None
    pnl_df = None
    cf_df = None

    for idx, tbl in enumerate(html_tables, start=1):
        df_list = pd.read_html(str(tbl))
        if not df_list:
            continue
        df = df_list[0]
        df.to_csv(out_dir / f"table_{idx}.csv", index=False)

        header_text = " ".join(map(str, df.columns)).lower()

        if any(w in header_text for w in ["баланс", "активы", "пассивы"]) and balance_df is None:
            balance_df = df
            df.to_csv(out_dir / "balance.csv", index=False)
        elif any(w in header_text for w in ["прибыли", "убытки", "финансовых результатов", "выручка"]) and pnl_df is None:
            pnl_df = df
            df.to_csv(out_dir / "otchet_o_finansovykh_rezultatakh.csv", index=False)
        elif any(w in header_text for w in ["движение денежных средств", "денежных средств", "cash flow"]) and cf_df is None:
            cf_df = df
            df.to_csv(out_dir / "otchet_o_dvizhenii_denezhnukh_sredstv.csv", index=False)

    print("Готово. CSV лежат в", out_dir)