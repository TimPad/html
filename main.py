"""
DataCulture Unified Platform - Main Navigation
"""
import streamlit as st

# =============================================================================
# КОНФИГУРАЦИЯ ПРИЛОЖЕНИЯ
# =============================================================================
st.set_page_config(
    page_title="DataCulture Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# APPLE-СОВМЕСТИМЫЙ UI/UX LAYER (Без изменения логики)
# =============================================================================
st.markdown("""
<style>
    /* Apple-style typography */
    h1, h2, h3, h4 {
        font-weight: 600;
        letter-spacing: -0.02em;
        line-height: 1.2;
        color: #e0e0e6;
    }
    h1 {
        font-size: 2.25rem;
        margin-bottom: 0.5rem;
    }
    h2 {
        font-size: 1.5rem;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    p, li, .stMarkdown {
        color: #c6c6cc;
        line-height: 1.6;
    }
    /* Apple-style buttons */
    .stButton > button {
        border-radius: 12px;
        padding: 8px 20px;
        font-weight: 500;
        border: none;
        background-color: #4a86e8;
        color: white;
        transition: background-color 0.2s ease;
    }
    .stButton > button:hover {
        background-color: #5a96f8;
    }
    /* Apple-style info boxes */
    .stAlert {
        border-radius: 12px;
        padding: 14px 18px;
        margin: 16px 0;
    }
    /* Sidebar refinement */
    [data-testid="stSidebar"] {
        background-color: #2a2a30 !important;
    }
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] label {
        color: #e0e0e6 !important;
    }
    /* Consistent spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    hr {
        margin: 1.5rem 0;
        border-color: #3a3a42;
    }
    /* Footer */
    footer {
        visibility: hidden;
    }
    .custom-footer {
        text-align: center;
        color: #888892;
        font-size: 0.85rem;
        margin-top: 2rem;
        padding-top: 1.5rem;
        border-top: 1px solid #3a3a42;
    }
</style>
""", unsafe_allow_html=True)

# Заголовок (Apple-style: без эмодзи, с caption)
st.title("DataCulture Platform")
st.caption("Объединённая платформа инструментов Data Culture @ HSE University")
st.caption("Для самых лучших сотрудников проекта от Тимошки")
st.markdown("---")

# --- Навигация ---
# Создаем список страниц для navigation
pages = [
    st.Page("page_grades.py", title="Перезачет оценок", icon="📊"),
    st.Page("page_cards.py", title="Генератор HTML-карточек", icon="🎓"),
    #st.Page("page_certificates.py", title="Генератор сертификатов", icon="📜"),
    #st.Page("page_resits.py", title="Обработка пересдач внешней оценки", icon="📝"),
    #st.Page("page_analytics.py", title="Аналитика курсов", icon="📈"),
    #st.Page("page_students.py", title="Обновление списка студентов", icon="👥"),
]

# Используем navigation в боковой панели
with st.sidebar:
    # Замени URL на свой логотип, если он доступен в этой директории
    # st.image("path/to/your/logo.svg", width=160) # Пока закомментировано
    st.markdown("<br>", unsafe_allow_html=True)
    # Создаем навигацию
    pg = navigation(pages)
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### ℹ️ О платформе")
    st.info("""
    **DataCulture Platform** объединяет шесть ключевых инструментов:
    1. **Перезачет оценок** — автоматический расчет итоговых оценок  
    2. **Генератор карточек** — создание HTML-рассылок в фирменном стиле ВШЭ  
    3. **Сертификаты** — обработка данных для выдачи сертификатов  
    4. **Пересдачи** — обработка пересдач внешней оценки  
    5. **Аналитика курсов** — обработка и загрузка аналитики курсов в Supabase  
    6. **Обновление студентов** — загрузка и обновление списка студентов в Supabase  
    """)

# Запускаем выбранную страницу
pg.run()

# Футер (Apple-style)
st.markdown("""
<div class="custom-footer">
    DataCulture Platform v1.0 · Created by Тимошка
</div>
""", unsafe_allow_html=True)
