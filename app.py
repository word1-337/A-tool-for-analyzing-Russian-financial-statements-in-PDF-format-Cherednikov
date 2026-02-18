# app.py

import streamlit as st
from pathlib import Path
from final_pdf_analyzer import process_pdf, RESULTS_DIR, BASE_DIR
import ollama  # если не хочешь ИИ-вывод, можно убрать и закомментить соответствующие блоки


st.set_page_config(page_title="Агент финансовой устойчивости", layout="wide")

st.title("Агент анализа финансовой устойчивости")

st.markdown(
    "Загрузи PDF с российской бухгалтерской отчётностью (баланс, ОФР, ОДДС). "
    "Файл будет сохранён в папку `source`, результат анализа — в папку `results`."
)

uploaded = st.file_uploader("Выбери файл PDF", type=["pdf"])

if uploaded is not None:
    # 1. Сохраняем входной PDF в source/
    source_dir = BASE_DIR / "source"
    source_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = source_dir / uploaded.name
    with open(pdf_path, "wb") as f:
        f.write(uploaded.getbuffer())

    st.success(f"Файл сохранён в {pdf_path}")

    if st.button("Проанализировать этот PDF"):
        with st.spinner("Запускаю MinerU и считаю показатели... (обработка может занять до 5 минут)"):
            process_pdf(pdf_path)

        st.session_state["last_pdf_stem"] = pdf_path.stem

# --------- показ отчёта и работа с Ollama ---------

if "last_pdf_stem" in st.session_state:
    stem = st.session_state["last_pdf_stem"]
    report_path = RESULTS_DIR / f"{stem}.txt"

    if report_path.is_file():
        report_text = report_path.read_text(encoding="utf-8")

        st.success(f"Анализ завершён. Результат сохранён в {report_path}")

        st.subheader("Отчёт по финансовой устойчивости")
        st.text_area(
            "Содержимое отчёта",
            value=report_text,
            height=400,
        )

        st.download_button(
            "Скачать отчёт (.txt)",
            data=report_text.encode("utf-8"),
            file_name=f"{stem}.txt",
            mime="text/plain",
        )

        # инициализация session_state для ответов Ollama
        if "ollama_summary" not in st.session_state:
            st.session_state["ollama_summary"] = ""
        if "ollama_answer" not in st.session_state:
            st.session_state["ollama_answer"] = ""

        # ----- краткий вывод -----
        st.markdown("---")
        st.subheader("Краткий вывод ИИ (Ollama)")

        if st.button("Сформировать краткий вывод"):
            try:
                with st.spinner("Генерирую вывод..."):
                    prompt = (
                        "Сделай краткий вывод о финансовой устойчивости компании на основе "
                        "следующего отчёта: оцени уровень риска (низкий/средний/высокий), "
                        "укажи ключевые сильные и слабые стороны.\n\n"
                        f"{report_text}"
                    )
                    resp = ollama.chat(
                        model="qwen2.5:32b",
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "Ты финансовый аналитик. Отвечай кратко, структурировано, "
                                    "на русском языке."
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ],
                    )
                    st.session_state["ollama_summary"] = resp["message"]["content"]
            except Exception as e:
                st.error(f"Ошибка при обращении к Ollama: {e}")

        if st.session_state["ollama_summary"]:
            st.write(st.session_state["ollama_summary"])

        # ----- вопросы к ИИ -----
        st.markdown("---")
        st.subheader("Задать свой вопрос ИИ по отчёту")

        question = st.text_input(
            "Вопрос по устойчивости (например: 'как изменилась выручка?' )",
            key="question_input",
        )

        if st.button("Спросить ИИ") and question:
            try:
                with st.spinner("Отвечаю..."):
                    prompt_q = (
                        "Ответь на вопрос по следующему отчёту о финансовой устойчивости.\n\n"
                        f"ОТЧЁТ:\n{report_text}\n\nВОПРОС: {question}"
                    )
                    resp_q = ollama.chat(
                        model="qwen2.5:32b",
                        messages=[
                            {
                                "role": "system",
                                "content": "Ты финансовый аналитик. Отвечай на русском.",
                            },
                            {"role": "user", "content": prompt_q},
                        ],
                    )
                    st.session_state["ollama_answer"] = resp_q["message"]["content"]
            except Exception as e:
                st.error(f"Ошибка при обращении к Ollama: {e}")

        if st.session_state["ollama_answer"]:
            st.write(st.session_state["ollama_answer"])
    else:
        st.info("Нажми кнопку 'Проанализировать этот PDF', чтобы получить отчёт.")