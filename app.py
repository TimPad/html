import streamlit as st
import json
from openai import OpenAI

# === Проверка секретов ===
if "NEBIUS_API_KEY" not in st.secrets:
    st.error("❌ Отсутствует NEBIUS_API_KEY в secrets.")
    st.stop()

# === Кэшированный клиент Nebius ===
@st.cache_resource
def get_nebius_client():
    return OpenAI(
        base_url="https://api.studio.nebius.com/v1/",
        api_key=st.secrets["NEBIUS_API_KEY"]
    )

client = get_nebius_client()

# === Ссылка на логотип (raw GitHub) ===
LOGO_URL = "https://raw.githubusercontent.com/TimPad/html/main/DC.svg"

# === Пример HTML-вывода с логотипом ===
EXAMPLE_HTML = f"""
<div style="font-family: 'Inter', 'Segoe UI', Roboto, Arial, sans-serif; max-width: 860px; margin: 40px auto; background: #ffffff; border-radius: 16px; box-shadow: 0 4px 14px rgba(0,0,0,0.08); border: 1px solid #e5ebf8; overflow: hidden;">
    <!-- HEADER -->
    <div style="background: #00256c; color: white; padding: 28px 32px; text-align: center;">
        <img src="{LOGO_URL}" alt="Логотип Data Culture" style="height: 48px; margin-bottom: 16px;">
        <p><span style="font-size: 1.6em; font-weight: 700;">ЗАГОЛОВОК ОБЪЯВЛЕНИЯ</span></p>
        <p style="margin-top: 8px; line-height: 1.5;">Краткое введение или контекст.</p>
    </div>

    <!-- CONTENT -->
    <div style="padding: 28px 32px; color: #111827; line-height: 1.65;">
        <p>Основной текст объявления...</p>
        <h3 style="color: #00256c;">Подзаголовок</h3>
        <ul style="margin: 12px 0 22px 22px;">
            <li>Пункт списка</li>
        </ul>

        <div style="background: #f4f6fb; border-radius: 10px; padding: 16px 20px; margin: 16px 0;">
            <p>Информационный блок</p>
        </div>

        <div style="background: #fff8e1; border-left: 4px solid #f59e0b; padding: 14px 18px; border-radius: 8px; margin-bottom: 20px;">
            <p style="margin: 0; font-weight: 600; color: #92400e;">⚠️ Внимание! Важное уточнение.</p>
        </div>

        <div style="background: #f0fdf4; border-left: 4px solid #16a34a; padding: 16px 20px; border-radius: 8px;">
            <p style="margin: 4px 0 0;"><strong>Удачи!</strong> 🚀</p>
        </div>
    </div>
</div>
""".strip()

# === Системное сообщение для LLM ===
SYSTEM_MESSAGE = (
    "Вы — эксперт по оформлению официальных рассылок НИУ ВШЭ. "
    "Ваша задача — преобразовать входной текст объявления в HTML-карточку, строго следуя фирменному стилю. "
    "В шапке обязательно должен быть логотип по ссылке: " + LOGO_URL + ". "
    "Используйте структуру и CSS-стили из приведённого ниже примера. "
    "Не добавляйте пояснений, комментариев или лишних тегов. "
    "Верните ТОЛЬКО корректный JSON в формате: {\"type\": \"HTML\", \"content\": \"<div>...</div>\"}.\n\n"
    "Пример корректного вывода:\n" + EXAMPLE_HTML
)

# === Функция генерации HTML ===
def generate_hse_html(client: OpenAI, user_text: str) -> str:
    response = client.chat.completions.create(
        model="Qwen/Qwen3-235B-A22B-Thinking-2507",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_MESSAGE},
            {"role": "user", "content": user_text}
        ],
        timeout=60.0
    )

    raw_content = response.choices[0].message.content.strip()

    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError:
        raise ValueError(f"Модель вернула не-JSON. Ответ:\n{raw_content[:500]}")

    if not isinstance(parsed, dict):
        raise ValueError("Ответ не является объектом JSON.")

    if parsed.get("type") != "HTML":
        raise ValueError("Поле 'type' должно быть 'HTML'.")

    content = parsed.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Поле 'content' отсутствует или пустое.")

    return content.strip()

# === Streamlit UI ===
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
                html_code = generate_hse_html(client, user_text)
                st.success("✅ Карточка успешно создана!")

                st.subheader("📄 HTML-код")
                st.code(html_code, language="html")

                st.download_button(
                    label="💾 Скачать HTML-файл",
                    data=html_code.encode("utf-8"),
                    file_name="hse_card.html",
                    mime="text/html"
                )

                st.subheader("🌐 Предпросмотр")
                st.components.v1.html(html_code, height=1000, scrolling=True)

            except Exception as e:
                st.error(f"❌ Ошибка при обработке: {e}")
else:
    st.info("Введите текст и нажмите «✨ Сформировать HTML», чтобы создать карточку.")
