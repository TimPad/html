import streamlit as st
import json
from openai import OpenAI
import os

# Инициализация клиента Nebius
client = OpenAI(
    base_url="https://api.studio.nebius.com/v1/",
    api_key=st.secrets["NEBIUS_API_KEY"]
)

# ======= Системный JSON-промт =======
SYSTEM_PROMPT = {
    "task": "format_announcement_hse_card",
    "description": "Преобразовать текст объявления или рассылки в структурированный HTML-блок в фирменном стиле НИУ ВШЭ (карточка с заголовками, блоками, списками, кнопками).",
    "instructions": {
        "steps": [
            "1. Прочитать исходный текст объявления из поля input_text.",
            "2. Разбить текст на логические разделы (введение, основная информация, инструкции, контакты и т.д.).",
            "3. Сократить длинные фразы, повысить читаемость и убрать избыточные повторы.",
            "4. Использовать официальный, дружелюбный стиль НИУ ВШЭ.",
            "5. Оформить результат в адаптивной HTML-карточке с цветами НИУ ВШЭ: тёмно-синий (#00256c), серо-голубой (#e5ebf8), белый фон, мягкие тени и округлые углы.",
            "6. Добавить логотип НИУ ВШЭ (https://www.hse.ru/static/images/hse_logo_white.svg) в шапку.",
            "7. Если есть ссылки — оформить их как кнопки внизу карточки.",
            "8. Сохранять структуру: h2/h3/h4, списки, выделения ключевых слов.",
            "9. Вернуть только чистый HTML-код карточки."
        ]
    },
    "style_guidelines": {
        "font_family": "'Inter', 'Segoe UI', Roboto, Arial, sans-serif",
        "primary_color": "#00256c",
        "secondary_color": "#e5ebf8",
        "accent_color": "#16a34a",
        "border_radius": "16px",
        "shadow": "0 4px 14px rgba(0,0,0,0.08)",
        "max_width": "860px",
        "include_logo": True
    },
    "output_format": "HTML"
}

# ======= Интерфейс Streamlit =======
st.set_page_config(page_title="HSE Email Formatter", page_icon="📧", layout="wide")

st.title("📧 Генератор карточек НИУ ВШЭ")
st.caption("Автоматическое оформление рассылок и объявлений в фирменном HTML-стиле ВШЭ.")

with st.form("input_form"):
    user_text = st.text_area("Введите текст объявления:", height=250, placeholder="Вставьте сюда текст письма или новости...")
    submitted = st.form_submit_button("✨ Сформировать HTML")

if submitted and user_text.strip():
    with st.spinner("Генерация HTML-карточки..."):
        try:
            # Формируем json-пакет для модели
            payload = SYSTEM_PROMPT.copy()
            payload["input_text"] = user_text

            response = client.chat.completions.create(
                model="Qwen/Qwen3-235B-A22B-Thinking-2507",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": json.dumps(payload, ensure_ascii=False)},
                    {"role": "user", "content": [{"type": "text", "text": user_text}]}
                ],
            )

            result = json.loads(response.choices[0].message.content)
            html_code = result.get("html", response.choices[0].message.content)

            st.success("✅ Карточка успешно создана!")

            # Отображение HTML-кода
            st.subheader("📄 HTML-код")
            st.code(html_code, language="html")

            # Предпросмотр
            st.subheader("🌐 Предпросмотр")
            st.components.v1.html(html_code, height=900, scrolling=True)

        except Exception as e:
            st.error(f"Ошибка: {e}")

else:
    st.info("Введите текст и нажмите «Сформировать HTML», чтобы начать.")
