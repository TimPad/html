import streamlit as st
import json
from openai import OpenAI
from io import BytesIO

# Инициализация клиента Nebius
client = OpenAI(
    base_url="https://api.studio.nebius.com/v1/",
    api_key=st.secrets["NEBIUS_API_KEY"]
)

# ======= Системный JSON-промт =======
SYSTEM_PROMPT = {
    "task": "format_announcement_hse_card",
    "description": "Преобразовать текст объявления или рассылки в структурированный HTML-блок в фирменном стиле НИУ ВШЭ.",
    "instructions": {
        "steps": [
            "1. Прочитать исходный текст объявления из поля input_text.",
            "2. Разбить текст на логические блоки.",
            "3. Сократить длинные фразы, сделать читаемо.",
            "4. Использовать официальный стиль ВШЭ.",
            "5. Вернуть JSON с полями {type: 'HTML', content: '<div>...</div>'}."
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
st.set_page_config(page_title="HSE HTML Generator", page_icon="🎓", layout="wide")
st.title("🎓 Генератор карточек НИУ ВШЭ")
st.caption("Создаёт HTML-карточку рассылки в фирменном стиле ВШЭ через Nebius API")

user_text = st.text_area(
    "Введите текст объявления:",
    height=250,
    placeholder="Вставьте сюда текст письма или новости..."
)

if st.button("✨ Сформировать HTML"):
    if not user_text.strip():
        st.warning("Введите текст, чтобы начать генерацию.")
    else:
        with st.spinner("Генерация карточки через Qwen3-235B..."):
            try:
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

                raw_content = response.choices[0].message.content.strip()
                parsed = json.loads(raw_content)

                # Если модель вернула {type: "HTML", content: "<div>...</div>"}
                html_code = parsed.get("content") if isinstance(parsed, dict) and "content" in parsed else raw_content

                st.success("✅ Карточка успешно создана!")

                # === Отображение HTML-кода ===
                st.subheader("📄 HTML-код")
                st.code(html_code, language="html")

                # === Кнопка для скачивания ===
                html_bytes = BytesIO(html_code.encode("utf-8"))
                st.download_button(
                    label="💾 Скачать HTML-файл",
                    data=html_bytes,
                    file_name="hse_card.html",
                    mime="text/html"
                )

                # === Предпросмотр ===
                st.subheader("🌐 Предпросмотр")
                st.components.v1.html(html_code, height=1000, scrolling=True)

            except Exception as e:
                st.error(f"❌ Ошибка при обработке ответа: {e}")
else:
    st.info("Введите текст и нажмите «✨ Сформировать HTML», чтобы создать карточку.")
