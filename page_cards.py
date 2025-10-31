# page_cards.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
from io import StringIO
import json
from openai import OpenAI
import tempfile
import os
from typing import Dict, Tuple
from supabase import create_client, Client
import xlsxwriter # Убедитесь, что он установлен: pip install xlsxwriter

# =============================================================================
# КОНСТАНТЫ
# =============================================================================
LOGO_URL = "https://raw.githubusercontent.com/TimPad/html/main/DC_green.svg"
HTML_EXAMPLE = f"""<div style="font-family: 'Inter', 'Segoe UI', Roboto, Arial, sans-serif; max-width: 860px; margin: 40px auto; background: #ffffff; border-radius: 16px; box-shadow: 0 4px 14px rgba(0,0,0,0.08); border: 1px solid #e5ebf8; overflow: hidden;">
    <div style="background: #00256c; color: white; padding: 28px 32px; text-align: center;">
        <img src="{LOGO_URL}" alt="Логотип Data Culture" style="height: 48px; margin-bottom: 16px;">
        <p><span style="font-size: 1.6em; font-weight: 700;">ЗАГОЛОВОК ОБЪЯВЛЕНИЯ</span></p>
        <p style="margin-top: 8px; line-height: 1.5;">Краткое введение или контекст.</p>
    </div>
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
</div>"""

# Используем json.dumps для правильного экранирования кавычек в JSON-строке
_json_example_str = json.dumps({"type": "HTML", "content": HTML_EXAMPLE}, ensure_ascii=False, indent=2)

SYSTEM_MESSAGE = (
    "Вы — эксперт по оформлению официальных рассылок НИУ ВШЭ. "
    "Ваша задача — преобразовать входной текст объявления в HTML-карточку, строго следуя фирменному стилю. "
    "В шапке обязательно должен быть логотип по ссылке: " + LOGO_URL + ". "
    "Используйте структуру и CSS-стили из приведённого ниже примера. "
    "Не добавляйте пояснений, комментариев или лишних тегов. "
    "Верните ТОЛЬКО корректный JSON в формате: {\"type\": \"HTML\", \"content\": \"<div>...</div>\"}.\n"
    "Пример корректного вывода:\n"
    f"{_json_example_str}"
)

# =============================================================================
# ФУНКЦИИ ДЛЯ МОДУЛЯ 2: ГЕНЕРАТОР HTML-КАРТОЧЕК
# =============================================================================
@st.cache_resource
def get_nebius_client():
    """Получить клиент Nebius API"""
    if "NEBIUS_API_KEY" not in st.secrets:
        raise ValueError("NEBIUS_API_KEY не найден в secrets.")
    return OpenAI(
        base_url="https://api.studio.nebius.com/v1/",
        api_key=st.secrets["NEBIUS_API_KEY"]
    )

def generate_hse_html(client: OpenAI, user_text: str) -> str:
    """
    Генерация HTML-карточки через Nebius API
    Args:
        client: OpenAI клиент
        user_text: Текст объявления
    Returns:
        HTML-код карточки
    """
    response = client.chat.completions.create(
        model="Qwen/Qwen3-Coder-30B-A3B-Instruct",
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

# =============================================================================
# ОСНОВНОЕ СОДЕРЖИМОЕ СТРАНИЦЫ
# =============================================================================
st.header("🎓 Генератор карточек НИУ ВШЭ")
st.markdown("""
Создайте HTML-карточку рассылки в фирменном стиле ВШЭ с помощью искусственного интеллекта.
**Как использовать:**
1. Введите текст объявления или новости
2. Нажмите кнопку генерации
3. Получите готовый HTML-код и предпросмотр
""")

try:
    has_api_key = "NEBIUS_API_KEY" in st.secrets
except FileNotFoundError:
    has_api_key = False

if not has_api_key:
    st.error("❌ NEBIUS_API_KEY не настроен. Обратитесь к администратору.")
    st.info("💡 Создайте файл `.streamlit/secrets.toml` с вашим API ключом")
    st.stop()

user_text = st.text_area(
    "Введите текст объявления:",
    height=250,
    placeholder="Вставьте сюда текст письма или новости..."
)

if st.button("✨ Сформировать HTML", type="primary"):
    if not user_text.strip():
        st.warning("⚠️ Введите текст для генерации")
    else:
        with st.spinner("Генерация карточки…"):
            try:
                client = get_nebius_client()
                html_code = generate_hse_html(client, user_text)
                st.success("✅ Карточка успешно создана!")
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.subheader("📄 HTML-код")
                    st.code(html_code, language="html")
                    st.download_button(
                        label="💾 Скачать HTML",
                        data=html_code.encode("utf-8"),
                        file_name="hse_card.html",
                        mime="text/html"
                    )
                with col2:
                    st.subheader("🌐 Предпросмотр")
                    import streamlit.components.v1 as components
                    components.html(html_code, height=800, scrolling=True)
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")
