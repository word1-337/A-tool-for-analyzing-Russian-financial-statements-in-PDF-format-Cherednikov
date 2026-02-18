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