import sqlite3
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as psycopg2_pool
except Exception:
    psycopg2 = None
    psycopg2_pool = None
    RealDictCursor = None
from pathlib import Path
from datetime import date, datetime
import shutil
import os
import hashlib
import hmac
import base64
import re
import html
import logging
import unicodedata

# Reduz avisos de depreciação do Streamlit no CMD.
# Eles não são erros de execução; são avisos internos de atualização futura.
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.deprecation_util").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


@st.cache_resource(show_spinner=False)
def get_pg_pool(database_url):
    """Reutiliza conexões PostgreSQL no Streamlit Cloud para reduzir lentidão."""
    if psycopg2 is None or psycopg2_pool is None:
        return None
    return psycopg2_pool.SimpleConnectionPool(
        minconn=1,
        maxconn=8,
        dsn=database_url,
        cursor_factory=RealDictCursor,
    )


DB_PATH = Path(__file__).with_name("mini_erp.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)
DEBUG_MIN_STOCK_LOG_PATH = Path(__file__).with_name("debug_min_stock.log")
DB_INTEGRITY_ERROR = (sqlite3.IntegrityError,) + ((psycopg2.IntegrityError,) if psycopg2 is not None else tuple())
ASSETS_DIR = Path(__file__).with_name("assets")
LOGO_PATH = ASSETS_DIR / "logo_subaquaticos.jpeg"
LOGO_TRANSPARENT_PATH = ASSETS_DIR / "logo_subaquaticos_transparente.png"
LOGO_LOGIN_PATH = ASSETS_DIR / "logo_subaquaticos_login.png"

CATEGORIAS_PADRAO = ["Bebida", "Comida", "Equipamento", "Operacional", "Outro"]
TIPOS_MOVIMENTACAO = ["Ajuste manual", "Perda/quebra/vencimento", "Consumo interno", "Devolução", "Outra entrada", "Outra saída"]

PERFIS_USUARIO = ["Administrador", "Caixa", "Estoque", "Financeiro", "Consulta"]
TEMAS_VISUAIS = ["Tactical Couple", "Tactical Clean"]
TEMA_PADRAO = "Tactical Couple"
MENU_POR_PERFIL = {
    "Administrador": ["Dashboard", "Caixa", "Jogos", "Operadores", "Comandas", "Produtos", "Estoque", "Despesas", "Relatórios", "Usuários", "Logs/Auditoria", "Configurações"],
    "Caixa": ["Dashboard", "Caixa", "Jogos", "Operadores", "Comandas", "Relatórios"],
    "Estoque": ["Dashboard", "Produtos", "Estoque", "Relatórios"],
    "Financeiro": ["Dashboard", "Caixa", "Jogos", "Despesas", "Relatórios", "Logs/Auditoria"],
    "Consulta": ["Dashboard", "Relatórios"],
}




def only_digits_11(value):
    """Mantém somente números e limita o resultado a 11 dígitos."""
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))[:11]


def only_digits(value, max_len=None):
    """Mantém somente números e, se informado, limita ao tamanho máximo."""
    if value is None:
        return ""
    digits = re.sub(r"\D", "", str(value))
    return digits[:max_len] if max_len else digits

def normalize_search_text(value):
    """Normaliza texto para pesquisa sem diferenciar acentos, caixa ou espaços extras."""
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text

def format_date_br(value):
    if value is None or value == "":
        return ""
    try:
        return pd.to_datetime(value).strftime("%d/%m/%Y")
    except Exception:
        return str(value)


def format_date_columns(df, columns=("Data",)):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(format_date_br)
    return df


FIELD_LABELS = {
    "Onde_mora": "Onde mora",
    "Jogo_evento": "Jogo / evento",
    "Operador_jogador": "Operador / jogador",
    "Jogador_cliente": "Jogador / cliente",
    "Tipo_entrada": "Tipo de entrada",
    "Valor_entrada": "Valor da entrada",
    "Desconto_%": "Desconto %",
    "Desconto_valor": "Valor do desconto",
    "Total_final": "Total final",
    "Total_jogadores": "Total de jogadores",
    "Custo_variável_por_jogador": "Custo variável por jogador",
    "Preço_unitário": "Preço unitário",
    "Custo_unitário": "Custo unitário",
    "Custo_médio": "Custo médio",
    "Valor_unitário": "Valor unitário",
    "Quantidade_comprada": "Quantidade comprada",
    "Data_compra": "Data da compra",
    "Forma_pagamento": "Forma de pagamento",
    "Valor_pago": "Valor pago",
    "Caixa_ID": "Caixa ID",
    "Operador_ID": "Operador ID",
}


def field_label(col):
    text = FIELD_LABELS.get(str(col), str(col).replace("_", " "))
    text = text.replace("  ", " ").strip()
    return text[:1].upper() + text[1:] if text else text


def pretty_columns(df):
    if isinstance(df, pd.DataFrame):
        return df.rename(columns={c: field_label(c) for c in df.columns})
    return df


def pretty_dataframe(df, *args, **kwargs):
    """Exibe DataFrame com nomes de colunas formatados, sem recursão."""
    return st.dataframe(pretty_columns(df), *args, **kwargs)


def render_field_tooltip(label, text):
    """Renderiza tooltip visual que aparece só no hover e não entra no fluxo do TAB."""
    safe_label = str(label).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_text = str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    st.markdown(
        f"""
        <div class="tactical-field-label">
            <span>{safe_label}</span>
            <span class="tactical-tooltip" tabindex="-1" aria-hidden="true">i
                <span class="tactical-tooltip-text">{safe_text}</span>
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def default_column_config(df, existing=None):
    config = dict(existing or {})
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            config.setdefault(col, st.column_config.TextColumn(field_label(col)))
    return config


def csv_download(df, filename, label):
    """Cria botão padronizado para baixar DataFrame em CSV.
    Usa UTF-8 com BOM para abrir corretamente no Excel em português.
    """
    if not can_export_data():
        st.caption("Exportação disponível apenas para Administrador ou Financeiro.")
        return
    if df is None:
        df = pd.DataFrame()
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label=label,
        data=csv_bytes,
        file_name=filename,
        mime="text/csv",
    )


def brl(valor):
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def parse_money_input(value):
    """Converte valores digitados em formato brasileiro ou simples para float.
    Exemplos aceitos: 150, 150.50, 150,50, R$ 1.250,75.
    """
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    text = text.replace("R$", "").replace(" ", "")
    text = re.sub(r"[^0-9,.-]", "", text)
    if "," in text and "." in text:
        # Formato brasileiro: 1.250,75
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        # Formato brasileiro simples: 150,75
        text = text.replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


def image_to_base64(path):
    try:
        if Path(path).exists():
            return base64.b64encode(Path(path).read_bytes()).decode("utf-8")
    except Exception:
        return ""
    return ""


def get_visual_theme():
    user = st.session_state.get("auth_user") or {}
    theme = user.get("visual_theme") or st.session_state.get("visual_theme") or TEMA_PADRAO
    return theme if theme in TEMAS_VISUAIS else TEMA_PADRAO


def tactical_clean_css():
    return """
        <style>
            :root {
                --tc-bg: #f7f7f7;
                --tc-bg-2: #ffffff;
                --tc-surface: #ffffff;
                --tc-surface-2: #f3f4f6;
                --tc-panel: #ffffff;
                --tc-line: #d9d9d9;
                --tc-line-soft: #e5e7eb;
                --tc-green: #111111;
                --tc-green-2: #1f1f1f;
                --tc-green-3: #333333;
                --tc-text: #111111;
                --tc-text-soft: #666666;
                --tc-sand: #c1121f;
                --tc-sand-2: #c1121f;
                --tc-danger: #c1121f;
                --tc-success: #1f7a3f;
                --tc-warning: #8a5a00;
            }

            .stApp {
                background: #f7f7f7 !important;
                color: #111111 !important;
            }

            h1, h2, h3, h4, h5, h6, .stMarkdown, label, p, span {
                color: #111111 !important;
            }

            [data-testid="stSidebar"] {
                background: #111111 !important;
                border-right: 1px solid #333333 !important;
            }
            [data-testid="stSidebar"] * {
                color: #f7f7f7 !important;
            }
            [data-testid="stSidebar"] .stCaptionContainer,
            [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
                color: #d1d5db !important;
            }
            [data-testid="stSidebar"] .stRadio label:hover {
                background: transparent !important;
                border-left-color: #c1121f !important;
            }
            [data-testid="stSidebar"] .stButton > button,
            [data-testid="stSidebar"] .stButton > button:hover {
                background: #ffffff !important;
                color: #111111 !important;
                border: 1px solid #d9d9d9 !important;
                border-radius: 0 !important;
                box-shadow: none !important;
                filter: none !important;
                transform: none !important;
            }
            [data-testid="stSidebar"] .stButton > button *,
            [data-testid="stSidebar"] .stButton > button:hover * {
                color: #111111 !important;
            }

            .sub-header {
                background: #ffffff !important;
                border: 1px solid #d9d9d9 !important;
                border-left: 5px solid #c1121f !important;
                box-shadow: none !important;
            }
            .sub-header h1, .sub-header p, .sub-header span, .sub-header div {
                color: #111111 !important;
            }
            .sub-header img {
                border: 1px solid #d9d9d9 !important;
            }

            .dashboard-hero {
                background: #ffffff !important;
                border: 1px solid #d9d9d9 !important;
                border-left: 5px solid #c1121f !important;
                box-shadow: none !important;
            }
            .dashboard-hero, .dashboard-hero * {
                color: #111111 !important;
            }
            .dashboard-status-pill {
                background: #111111 !important;
                border: 1px solid #111111 !important;
                color: #ffffff !important;
            }
            .dashboard-status-pill * {
                color: #ffffff !important;
            }

            .dash-kpi, .dash-card, .tc-panel, div[data-testid="metric-container"] {
                background: #ffffff !important;
                border: 1px solid #d9d9d9 !important;
                border-top: 3px solid #111111 !important;
                box-shadow: none !important;
                color: #111111 !important;
            }
            .dash-kpi *, .dash-card *, .tc-panel *, div[data-testid="metric-container"] * {
                color: #111111 !important;
            }
            .dash-kpi-icon, .dash-kpi-icon * {
                background: #111111 !important;
                color: #ffffff !important;
                border-color: #111111 !important;
            }
            .dash-card-title, .dash-section-title, .tc-section-title {
                color: #111111 !important;
                border-color: #d9d9d9 !important;
            }
            .dash-section-title {
                background: #ffffff !important;
                border-left: 4px solid #c1121f !important;
            }

            .stTextInput input, .stNumberInput input, .stDateInput input, textarea, .stTextArea textarea,
            .stSelectbox [data-baseweb="select"] > div,
            .stMultiSelect [data-baseweb="select"] > div,
            [data-baseweb="base-input"] > div, div[data-baseweb="select"] > div {
                background: #ffffff !important;
                color: #111111 !important;
                border: 1px solid #d9d9d9 !important;
                border-radius: 0 !important;
                box-shadow: none !important;
            }

            .stButton > button, .stFormSubmitButton > button, .stDownloadButton > button,
            .stButton > button:hover, .stFormSubmitButton > button:hover, .stDownloadButton > button:hover {
                background: #111111 !important;
                color: #ffffff !important;
                border: 1px solid #111111 !important;
                border-radius: 0 !important;
                box-shadow: none !important;
                transform: none !important;
                filter: none !important;
            }
            .stButton > button *, .stFormSubmitButton > button *, .stDownloadButton > button * {
                color: #ffffff !important;
            }

            .stTabs [data-baseweb="tab"] {
                color: #111111 !important;
                border-radius: 0 !important;
            }
            .stTabs [aria-selected="true"] {
                color: #c1121f !important;
                border-bottom-color: #c1121f !important;
            }

            [data-testid="stDataFrame"], div[data-testid="stTable"], div[data-testid="stDataEditor"] {
                border: 1px solid #d9d9d9 !important;
                box-shadow: none !important;
            }
            [data-testid="stDataFrame"] [role="columnheader"],
            [data-testid="stTable"] thead th,
            div[data-testid="stDataEditor"] [role="columnheader"] {
                background: #111111 !important;
                color: #ffffff !important;
            }
            [data-testid="stDataFrame"] [role="gridcell"],
            div[data-testid="stDataEditor"] [role="gridcell"] {
                background: #ffffff !important;
                color: #111111 !important;
            }

            .login-title, .login-subtitle {
                color: #111111 !important;
            }
            hr {
                border-color: #d9d9d9 !important;
            }
        </style>
    """


def apply_brand_styles():
    """Tema Tactical Couple aplicado em todo o sistema."""
    st.markdown(
        f"""
        <style>
            :root {{
                --tc-bg: #d8ccb3;
                --tc-bg-2: #efe5d2;
                --tc-surface: #e8dcc3;
                --tc-surface-2: #f2e8d6;
                --tc-panel: #f5ecd9;
                --tc-line: #a89771;
                --tc-line-soft: #c7b58d;
                --tc-green: #263428;
                --tc-green-2: #314132;
                --tc-green-3: #425244;
                --tc-text: #1f261f;
                --tc-text-soft: #595541;
                --tc-sand: #c9b58d;
                --tc-sand-2: #d8c49b;
                --tc-danger: #8f2f2f;
                --tc-success: #2f5a34;
                --tc-warning: #7b5a20;
            }}

            .stApp {{
                background:
                    linear-gradient(135deg, #e2d6bd 0%, #efe5d2 38%, #dbcfb8 100%);
                color: var(--tc-text);
            }}

            html, body, [class*="css"] {{
                font-family: "Inter", "Segoe UI", Arial, sans-serif;
            }}

            h1, h2, h3, h4, h5, h6, .stMarkdown, label, p, span {{
                color: var(--tc-text);
            }}

            h1, h2, h3 {{
                letter-spacing: -0.02em;
                text-transform: none;
            }}

            [data-testid="stAppViewContainer"] > .main {{
                padding-top: .6rem;
            }}

            .block-container {{
                padding-top: 1.2rem;
                padding-bottom: 1.5rem;
                max-width: 1480px;
            }}

            [data-testid="stSidebar"] {{
                background:
                    linear-gradient(180deg, #263428 0%, #2a382c 35%, #314132 100%);
                border-right: 1px solid rgba(201,181,141,.28);
            }}

            [data-testid="stSidebar"] * {{
                color: #efe4cf !important;
            }}

            [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
            [data-testid="stSidebar"] .stCaptionContainer {{
                color: rgba(239,228,207,.84) !important;
            }}

            [data-testid="stSidebar"] img {{
                border-radius: 0 !important;
                box-shadow: none !important;
                border: 0 !important;
                background: transparent !important;
            }}

            [data-testid="stSidebar"] .stRadio label,
            [data-testid="stSidebar"] .stButton > button,
            [data-testid="stSidebar"] .stDownloadButton > button {{
                border-radius: 0 !important;
            }}

            [data-testid="stSidebar"] .stRadio label {{
                padding: .28rem .35rem;
                border-left: 3px solid transparent;
            }}

            [data-testid="stSidebar"] .stRadio label:hover {{
                background: rgba(201,181,141,.10);
                border-left-color: var(--tc-sand);
            }}

            [data-testid="stSidebar"] .stButton > button {{
                border: 1px solid #c9b58d !important;
                background: #d8c49b !important;
                color: #263428 !important;
                box-shadow: none !important;
                font-weight: 700 !important;
            }}

            [data-testid="stSidebar"] .stButton > button:hover {{
                border: 1px solid #c9b58d !important;
                background: #d8c49b !important;
                color: #263428 !important;
                box-shadow: none !important;
                transform: none !important;
                filter: none !important;
            }}

            .sidebar-anchor {{
                text-align: center;
                font-size: 1.8rem;
                color: #d8c49b;
                margin-top: 16px;
                opacity: .9;
            }}

            .sub-header {{
                display: flex;
                align-items: center;
                gap: 16px;
                padding: 16px 20px;
                margin: 4px 0 18px 0;
                background: linear-gradient(90deg, #263428 0%, #314132 58%, #425244 100%);
                border: 1px solid #758063;
                border-left: 6px solid var(--tc-sand);
                border-radius: 0;
                box-shadow: 0 10px 22px rgba(18,23,18,.16);
            }}
            .sub-header img {{
                width: 60px;
                height: 60px;
                object-fit: cover;
                border-radius: 0;
                border: 1px solid rgba(201,181,141,.30);
            }}
            .sub-header h1 {{
                color: #f3ead7 !important;
                font-size: 1.8rem;
                margin: 0;
                line-height: 1.15;
                font-weight: 900;
                text-transform: uppercase;
                letter-spacing: .01em;
            }}
            .sub-header p {{
                color: rgba(239,228,207,.84);
                margin: 4px 0 0 0;
                font-size: .94rem;
            }}

            .login-shell {{
                max-width: 480px;
                margin: 0 auto;
                padding: 18px 10px 8px;
                background: transparent !important;
                border: 0 !important;
                box-shadow: none !important;
                text-align: center;
                position: relative;
                overflow: visible;
            }}
            .login-shell::before,
            .login-shell::after {{
                content: none !important;
                display: none !important;
            }}
            .login-shell > * {{ position: relative; z-index: 1; }}
            .login-logo {{
                width: 300px;
                max-width: 86%;
                border-radius: 0 !important;
                margin: 0 auto 18px;
                display: block;
                background: transparent !important;
                border: 0 !important;
                box-shadow: none !important;
                padding: 0 !important;
                filter: drop-shadow(0 14px 18px rgba(18,23,18,.24));
            }}
            .login-title {{
                font-size: 2.7rem;
                font-weight: 900;
                color: var(--tc-green);
                margin: 2px 0 0;
                letter-spacing: -0.04em;
                text-transform: uppercase;
            }}
            .login-subtitle {{
                color: var(--tc-text-soft);
                margin-bottom: 28px;
                font-weight: 700;
            }}

            div[data-testid="stForm"] {{
                background: transparent;
                border: 0;
                padding: 0;
            }}

            .stTextInput input,
            .stNumberInput input,
            .stDateInput input,
            textarea,
            .stTextArea textarea,
            .stSelectbox [data-baseweb="select"] > div,
            .stMultiSelect [data-baseweb="select"] > div,
            [data-baseweb="base-input"] > div,
            div[data-baseweb="select"] > div {{
                border-radius: 0 !important;
                border: 1px solid var(--tc-line-soft) !important;
                background-color: rgba(245,236,217,.96) !important;
                color: var(--tc-text) !important;
                box-shadow: none !important;
                min-height: 42px;
            }}

            .stTextInput input:focus,
            .stNumberInput input:focus,
            .stDateInput input:focus,
            textarea:focus,
            .stTextArea textarea:focus,
            .stSelectbox [data-baseweb="select"] > div:focus-within,
            .stMultiSelect [data-baseweb="select"] > div:focus-within,
            [data-baseweb="base-input"] > div:focus-within {{
                border-color: var(--tc-green-2) !important;
                box-shadow: 0 0 0 1px var(--tc-green-2) inset !important;
            }}

            .stButton > button,
            .stFormSubmitButton > button,
            .stDownloadButton > button {{
                border-radius: 0 !important;
                border: 1px solid #415044 !important;
                background: linear-gradient(180deg, #314132 0%, #263428 100%) !important;
                color: #f3ead7 !important;
                font-weight: 800 !important;
                text-transform: uppercase !important;
                letter-spacing: .03em !important;
                min-height: 40px !important;
                box-shadow: none !important;
                padding: .45rem .95rem !important;
            }}
            .stButton > button:hover,
            .stFormSubmitButton > button:hover,
            .stDownloadButton > button:hover {{
                border-color: #c9b58d !important;
                background: linear-gradient(180deg, #3a4a3b 0%, #2a382c 100%) !important;
                color: #fff7e8 !important;
                transform: none !important;
                filter: none !important;
            }}

            .stButton > button[kind="secondary"],
            button[data-testid="baseButton-secondary"] {{
                background: linear-gradient(180deg, #d8c49b 0%, #c9b58d 100%) !important;
                color: #223022 !important;
                border-color: #8f7d58 !important;
            }}

            div[data-testid="metric-container"] {{
                background: linear-gradient(180deg, #f1e7d4 0%, #e7dbc4 100%);
                border: 1px solid var(--tc-line-soft);
                border-top: 4px solid var(--tc-green-2);
                border-radius: 0;
                padding: 16px 16px;
                box-shadow: none;
            }}
            div[data-testid="metric-container"] label {{
                color: var(--tc-text-soft) !important;
                font-weight: 800 !important;
                text-transform: uppercase;
                letter-spacing: .04em;
                font-size: .75rem !important;
            }}
            div[data-testid="metric-container"] [data-testid="stMetricValue"] {{
                color: var(--tc-green) !important;
                font-weight: 900 !important;
            }}

            [data-testid="stDataFrame"],
            div[data-testid="stTable"],
            div[data-testid="stDataEditor"] {{
                border-radius: 0 !important;
                overflow: hidden;
                border: 1px solid var(--tc-line-soft);
                box-shadow: none;
            }}
            [data-testid="stDataFrame"] [role="columnheader"],
            [data-testid="stTable"] thead th,
            div[data-testid="stDataEditor"] [role="columnheader"] {{
                font-weight: 900 !important;
                color: var(--tc-green) !important;
                text-transform: uppercase;
                letter-spacing: .03em;
                background: #dfd1b4 !important;
            }}
            div[data-testid="stDataEditor"] [role="gridcell"],
            [data-testid="stDataFrame"] [role="gridcell"] {{
                background: #f5ecd9 !important;
            }}

            .stTabs [data-baseweb="tab-list"] {{
                gap: 0;
                border-bottom: 1px solid var(--tc-line-soft);
            }}
            .stTabs [data-baseweb="tab"] {{
                border-radius: 0;
                color: var(--tc-text-soft);
                padding-left: 0;
                padding-right: 1rem;
            }}
            .stTabs [aria-selected="true"] {{
                color: var(--tc-green) !important;
                font-weight: 900;
            }}

            .stAlert {{
                border-radius: 0;
                border: 1px solid var(--tc-line-soft);
            }}

            .tactical-field-label {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                margin: 0 0 .28rem 0;
                font-weight: 700;
                color: var(--tc-text);
                font-size: .95rem;
                text-transform: none;
            }}
            .tactical-tooltip {{
                position: relative;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 18px;
                height: 18px;
                border-radius: 0;
                background: rgba(49,65,50,.12);
                border: 1px solid rgba(49,65,50,.25);
                color: var(--tc-green-2);
                font-size: .78rem;
                font-weight: 900;
                line-height: 1;
                cursor: help;
                user-select: none;
                outline: none;
                pointer-events: auto;
            }}
            .tactical-tooltip:focus {{
                outline: none !important;
                box-shadow: none !important;
            }}
            .tactical-tooltip-text {{
                visibility: hidden;
                opacity: 0;
                width: min(300px, 82vw);
                background: #263428;
                color: #fff8ed;
                text-align: left;
                border-radius: 0;
                padding: 9px 11px;
                position: absolute;
                z-index: 999999;
                bottom: calc(100% + 8px);
                left: 50%;
                transform: translateX(-50%);
                transition: opacity .15s ease, visibility .15s ease;
                font-size: .82rem;
                line-height: 1.35;
                font-weight: 500;
                box-shadow: none;
                border: 1px solid #6e7c69;
                white-space: normal;
            }}
            .tactical-tooltip:hover .tactical-tooltip-text {{
                visibility: visible;
                opacity: 1;
            }}
            .tactical-tooltip-text::after {{
                content: "";
                position: absolute;
                top: 100%;
                left: 50%;
                transform: translateX(-50%);
                border-width: 6px;
                border-style: solid;
                border-color: #263428 transparent transparent transparent;
            }}

            /* Containers auxiliares para todo o sistema */
            .tc-panel {{
                background: linear-gradient(180deg, #f2e8d6 0%, #e8dcc3 100%);
                border: 1px solid var(--tc-line-soft);
                border-top: 4px solid var(--tc-green-2);
                padding: 16px;
                border-radius: 0;
                box-shadow: none;
                margin-bottom: 14px;
            }}
            .tc-section-title {{
                font-size: 1.02rem;
                font-weight: 900;
                color: var(--tc-green);
                margin: 0 0 12px 0;
                text-transform: uppercase;
                letter-spacing: .04em;
                padding-bottom: 7px;
                border-bottom: 2px solid var(--tc-line-soft);
            }}

            /* Tactical Couple — dashboard */
            .dashboard-hero {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 18px;
                padding: 20px 22px;
                margin: 4px 0 18px 0;
                background: linear-gradient(90deg, #263428 0%, #324334 55%, #425244 100%);
                border: 1px solid #6f785d;
                border-left: 6px solid #c9b58d;
                border-radius: 0;
                box-shadow: 0 10px 22px rgba(18, 23, 18, .16);
            }}
            .dashboard-hero h2 {{
                margin: 0;
                color: #f3ead7 !important;
                font-size: 1.95rem;
                font-weight: 900;
                text-transform: uppercase;
                letter-spacing: .02em;
            }}
            .dashboard-hero p {{
                margin: 6px 0 0;
                color: #d8ccb1;
                font-size: .94rem;
            }}
            .dashboard-status-pill {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 10px 12px;
                border-radius: 0;
                background: #d2c09a;
                border: 1px solid #8f7d58;
                color: #263428;
                font-weight: 850;
                text-transform: uppercase;
                letter-spacing: .04em;
                white-space: nowrap;
            }}
            .dash-card {{
                background: linear-gradient(180deg, #f0e7d5 0%, #e7dbc2 100%);
                border: 1px solid #c5b28e;
                border-radius: 0;
                padding: 18px;
                box-shadow: 0 6px 16px rgba(33, 42, 34, .08);
                margin-bottom: 16px;
            }}
            .dash-card-title {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-weight: 900;
                font-size: 1.02rem;
                color: #263428;
                margin: 0 0 12px 0;
                padding-bottom: 6px;
                border-bottom: 2px solid #c7b58d;
                text-transform: uppercase;
                letter-spacing: .02em;
            }}
            .dash-kpi {{
                background: linear-gradient(180deg, #f3ead8 0%, #eadfc8 100%);
                border: 1px solid #beac86;
                border-top: 4px solid #314132;
                border-radius: 0;
                padding: 17px 18px;
                box-shadow: 0 8px 18px rgba(28, 36, 28, .10);
                min-height: 126px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            }}
            .dash-kpi-top {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
            }}
            .dash-kpi-icon {{
                width: 44px;
                height: 44px;
                border-radius: 0;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: #314132;
                color: #efe4cf;
                font-size: 1.25rem;
                border: 1px solid #566551;
            }}
            .dash-kpi-title {{
                font-size: .78rem;
                color: #5c563f;
                font-weight: 850;
                text-transform: uppercase;
                letter-spacing: .05em;
                margin-top: 2px;
            }}
            .dash-kpi-value {{
                color: #223022;
                font-size: 1.7rem;
                font-weight: 950;
                letter-spacing: -.03em;
                margin-top: 8px;
                line-height: 1.1;
            }}
            .dash-kpi-sub {{
                color: #66604a;
                font-size: .84rem;
                margin-top: 10px;
                line-height: 1.3;
            }}
            .dash-section-title {{
                font-size: 1.04rem;
                font-weight: 950;
                color: #263428;
                margin: 14px 0 12px;
                text-transform: uppercase;
                letter-spacing: .04em;
                padding: 8px 10px;
                background: linear-gradient(90deg, rgba(49,65,50,.10) 0%, rgba(201,181,141,.18) 100%);
                border-left: 4px solid #314132;
            }}
            .dash-action-row {{
                display: flex;
                align-items: center;
                justify-content: flex-end;
                gap: 10px;
                margin-top: 10px;
            }}

            /* Extensão v79 — aplicar paleta Tactical Couple sem alterar estrutura */
            div[data-testid="stHeadingWithActionElements"] h1,
            div[data-testid="stHeadingWithActionElements"] h2,
            div[data-testid="stHeadingWithActionElements"] h3 {{
                color: var(--tc-green) !important;
            }}

            .sub-header,
            .sub-header *,
            .dashboard-hero,
            .dashboard-hero *,
            [data-testid="stSidebar"],
            [data-testid="stSidebar"] * {{
                color: #f3ead7 !important;
            }}

            .dashboard-status-pill,
            .dashboard-status-pill * {{
                color: var(--tc-green) !important;
            }}

            .stTextInput input,
            .stNumberInput input,
            .stDateInput input,
            textarea,
            .stTextArea textarea,
            .stSelectbox [data-baseweb="select"] > div,
            .stMultiSelect [data-baseweb="select"] > div,
            [data-baseweb="base-input"] > div,
            div[data-baseweb="select"] > div {{
                background: #f5ecd9 !important;
                color: var(--tc-text) !important;
                border-color: var(--tc-line-soft) !important;
                border-radius: 0 !important;
            }}

            .stButton > button,
            .stFormSubmitButton > button,
            .stDownloadButton > button {{
                background: linear-gradient(180deg, var(--tc-green-2) 0%, var(--tc-green) 100%) !important;
                color: #f3ead7 !important;
                border-color: #415044 !important;
                border-radius: 0 !important;
                box-shadow: none !important;
            }}
            .stButton > button:hover,
            .stFormSubmitButton > button:hover,
            .stDownloadButton > button:hover {{
                background: linear-gradient(180deg, var(--tc-green-2) 0%, var(--tc-green) 100%) !important;
                color: #f3ead7 !important;
                border-color: #415044 !important;
                transform: none !important;
                filter: none !important;
            }}

            .stTabs [data-baseweb="tab"] {{
                color: var(--tc-text-soft) !important;
                border-radius: 0 !important;
            }}
            .stTabs [aria-selected="true"],
            .stTabs [aria-selected="true"] * {{
                color: var(--tc-green) !important;
                font-weight: 900 !important;
            }}

            [data-testid="stDataFrame"],
            div[data-testid="stTable"],
            div[data-testid="stDataEditor"] {{
                border-color: var(--tc-line-soft) !important;
                border-radius: 0 !important;
                box-shadow: none !important;
            }}
            [data-testid="stDataFrame"] [role="columnheader"],
            [data-testid="stTable"] thead th,
            div[data-testid="stDataEditor"] [role="columnheader"] {{
                color: var(--tc-green) !important;
                background: #dfd1b4 !important;
            }}
            [data-testid="stDataFrame"] [role="gridcell"],
            div[data-testid="stDataEditor"] [role="gridcell"] {{
                color: var(--tc-text) !important;
                background: #f5ecd9 !important;
            }}

            div[data-testid="metric-container"],
            .dash-card,
            .dash-kpi,
            .tc-panel {{
                background: linear-gradient(180deg, #f2e8d6 0%, #e8dcc3 100%) !important;
                color: var(--tc-text) !important;
                border-color: var(--tc-line-soft) !important;
                border-radius: 0 !important;
            }}
            div[data-testid="metric-container"] *,
            .dash-card *,
            .dash-kpi *,
            .tc-panel * {{
                color: var(--tc-text) !important;
            }}
            .dash-kpi-icon,
            .dash-kpi-icon * {{
                color: #f3ead7 !important;
            }}

            div[data-testid="stAlert"] {{
                border-radius: 0 !important;
                border-color: var(--tc-line-soft) !important;
            }}
            div[data-testid="stAlert"] * {{
                color: var(--tc-text) !important;
            }}

            .tactical-tooltip-text,
            .tactical-tooltip-text * {{
                color: #f3ead7 !important;
            }}

            /* Form layout e espaçamento mais militar/reto */
            .stTextInput, .stNumberInput, .stSelectbox, .stDateInput, .stTextArea, .stMultiSelect {{
                margin-bottom: .4rem;
            }}
            .stMarkdown p {{
                line-height: 1.45;
            }}
            hr {{ border-color: rgba(92, 86, 63, .22) !important; }}


            /* Correção v82: botão Sair do menu lateral claro com texto verde escuro, sem hover */
            [data-testid="stSidebar"] .stButton > button,
            [data-testid="stSidebar"] .stButton > button:hover,
            [data-testid="stSidebar"] .stButton > button:focus,
            [data-testid="stSidebar"] .stButton > button:active {{
                background: #d8c49b !important;
                color: #263428 !important;
                border: 1px solid #c9b58d !important;
                box-shadow: none !important;
                transform: none !important;
                filter: none !important;
            }}
            [data-testid="stSidebar"] .stButton > button *,
            [data-testid="stSidebar"] .stButton > button:hover *,
            [data-testid="stSidebar"] .stButton > button:focus *,
            [data-testid="stSidebar"] .stButton > button:active * {{
                color: #263428 !important;
                fill: #263428 !important;
            }}

            /* Correção v83: todos os botões padrão ficam claros, com escrita verde escuro e sem hover */
            .stButton > button,
            .stButton > button:hover,
            .stButton > button:focus,
            .stButton > button:active,
            .stFormSubmitButton > button,
            .stFormSubmitButton > button:hover,
            .stFormSubmitButton > button:focus,
            .stFormSubmitButton > button:active,
            .stDownloadButton > button,
            .stDownloadButton > button:hover,
            .stDownloadButton > button:focus,
            .stDownloadButton > button:active,
            button[data-testid="baseButton-secondary"],
            button[data-testid="baseButton-primary"] {{
                background: #d8c49b !important;
                color: #263428 !important;
                border: 1px solid #8f7d58 !important;
                border-radius: 0 !important;
                box-shadow: none !important;
                text-transform: uppercase !important;
                font-weight: 700 !important;
                letter-spacing: .03em !important;
                transform: none !important;
                filter: none !important;
            }}

            .stButton > button *,
            .stButton > button:hover *,
            .stButton > button:focus *,
            .stButton > button:active *,
            .stFormSubmitButton > button *,
            .stFormSubmitButton > button:hover *,
            .stFormSubmitButton > button:focus *,
            .stFormSubmitButton > button:active *,
            .stDownloadButton > button *,
            .stDownloadButton > button:hover *,
            .stDownloadButton > button:focus *,
            .stDownloadButton > button:active *,
            button[data-testid="baseButton-secondary"] *,
            button[data-testid="baseButton-primary"] * {{
                color: #263428 !important;
                fill: #263428 !important;
            }}



            /* Cards dos itens da comanda */
            .command-item-card {{
                background: transparent;
                border: none;
                border-left: 4px solid var(--tc-green-2, #314132);
                border-radius: 0;
                padding: 4px 0 2px 14px;
                margin: 0 0 10px 0;
                color: var(--tc-text, #1f261f);
            }}
            .command-item-title {{
                font-weight: 900;
                font-size: 1.04rem;
                color: var(--tc-green, #263428) !important;
                text-transform: uppercase;
                letter-spacing: .02em;
                margin-bottom: 4px;
            }}
            .command-item-meta,
            .command-item-row,
            .command-item-notes {{
                color: var(--tc-text-soft, #595541) !important;
                font-size: .92rem;
                line-height: 1.45;
            }}
            .command-item-row strong,
            .command-item-notes strong {{
                color: var(--tc-text, #1f261f) !important;
            }}
            .command-item-total {{
                color: var(--tc-green, #263428) !important;
                font-weight: 900;
            }}

            .dashboard-kpi-grid {{
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 14px;
                align-items: stretch;
                margin-bottom: 14px;
            }}
            .dashboard-kpi-grid .dash-kpi {{
                height: 100%;
                min-height: 128px;
                box-sizing: border-box;
            }}

            .product-card-low {{
                background: rgba(190, 50, 50, 0.12) !important;
                border-color: rgba(160, 35, 35, 0.35) !important;
            }}
            .product-card-low .command-item-card {{
                border-left-color: rgba(160, 35, 35, 0.75) !important;
            }}
            .product-card-box {{
                min-height: 154px;
                padding: 2px 0 0 0;
            }}
            .product-card-actions {{
                margin-top: 8px;
            }}
            .product-card-box .command-item-title {{
                font-size: 1rem;
            }}

            @media (max-width: 1100px) {{
                .dashboard-kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
            }}
            @media (max-width: 650px) {{
                .dashboard-kpi-grid {{ grid-template-columns: 1fr; }}
                .dashboard-kpi-grid .dash-kpi {{ min-height: auto; }}
            }}

            @media (max-width: 900px) {{
                .dashboard-hero {{ flex-direction: column; align-items: flex-start; }}
                .dashboard-status-pill {{ white-space: normal; width: 100%; justify-content: center; }}
                .dash-kpi {{ min-height: auto; }}
                .dash-kpi-value {{ font-size: 1.45rem; }}
                .sub-header {{ padding: 14px 16px; }}
                .sub-header img {{ width: 54px; height: 54px; }}
                .sub-header h1 {{ font-size: 1.45rem; }}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    if get_visual_theme() == "Tactical Clean":
        st.markdown(tactical_clean_css(), unsafe_allow_html=True)


def render_brand_header():
    header_logo = LOGO_TRANSPARENT_PATH if LOGO_TRANSPARENT_PATH.exists() else LOGO_PATH
    logo_b64 = image_to_base64(header_logo)
    mime = "png" if str(header_logo).lower().endswith(".png") else "jpeg"
    img_html = f'<img src="data:image/{mime};base64,{logo_b64}" alt="Subaquáticos">' if logo_b64 else '<div></div>'
    st.markdown(
        f"""
        <div class="sub-header">
            {img_html}
            <div>
                <h1>Tactical</h1>
                <p>A gestão tática do seu campo.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



def using_postgres():
    """Retorna True quando o app está rodando com banco PostgreSQL online."""
    return USE_POSTGRES


class PgCursorCompat:
    """Adaptador mínimo para deixar cursores PostgreSQL parecidos com sqlite3.Row."""
    def __init__(self, cursor, lastrowid=None, rows=None):
        self._cursor = cursor
        self.lastrowid = lastrowid
        self._rows = rows

    def fetchone(self):
        if self._rows is not None:
            return self._rows[0] if self._rows else None
        return self._cursor.fetchone()

    def fetchall(self):
        if self._rows is not None:
            return self._rows
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self.fetchall())


class PgConnCompat:
    """Compatibilidade básica para usar conn.execute(...) no PostgreSQL."""
    def __init__(self):
        if psycopg2 is None:
            raise RuntimeError("psycopg2-binary não está instalado. Adicione ao requirements.txt para usar PostgreSQL.")
        self._pool = get_pg_pool(DATABASE_URL)
        if self._pool is None:
            raise RuntimeError("Pool PostgreSQL não pôde ser inicializado.")
        self._conn = self._pool.getconn()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self._conn.rollback()
        self._pool.putconn(self._conn)
        return False

    def _translate_sql(self, sql):
        """Converte SQL escrito para SQLite para PostgreSQL sem alterar a lógica da query.

        O app original foi criado em SQLite. Para a versão cloud, mantemos as queries
        do app e adaptamos apenas o necessário:
        - placeholders ? -> %s
        - PRAGMA/sqlite_sequence ignorados
        - strftime simples -> to_char
        - aliases do SELECT preservados com aspas, porque o PostgreSQL transforma
          aliases não cotados em minúsculas e o restante do app espera nomes como
          Data, Total, Valor, Receita, Nome etc.
        - % literal de LIKE escapado para psycopg2.
        """
        sql = str(sql)
        stripped = sql.strip()
        upper = stripped.upper()
        if upper.startswith("PRAGMA") or "SQLITE_SEQUENCE" in upper:
            return None

        sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        sql = sql.replace("strftime('%d/%m/%Y', e.event_date)", "to_char(e.event_date::date, 'DD/MM/YYYY')")
        sql = sql.replace('strftime("%d/%m/%Y", e.event_date)', "to_char(e.event_date::date, 'DD/MM/YYYY')")
        sql = sql.replace("?", "%s")

        aliases = []

        def quote_alias(match):
            alias = match.group(1)
            aliases.append(alias)
            return f'AS "{alias}"'

        # Preserva aliases em SELECT ... AS Alias.
        sql = re.sub(
            r'\bAS\s+([A-Za-z_À-ÿ][A-Za-z0-9_À-ÿ]*)',
            quote_alias,
            sql,
            flags=re.IGNORECASE,
        )

        # Se a query ordenar por um alias, o PostgreSQL precisa do alias cotado.
        for alias in sorted(set(aliases), key=len, reverse=True):
            sql = re.sub(
                rf'(?<![".])\b{re.escape(alias)}\b(?!")',
                f'"{alias}"',
                sql,
            )

        # psycopg2 usa %s como placeholder; % literal de LIKE deve ser %%.
        sql = re.sub(r'%(?!s)', '%%', sql)
        return sql

    def execute(self, sql, params=()):
        translated = self._translate_sql(sql)
        if translated is None:
            return PgCursorCompat(None, rows=[])
        cur = self._conn.cursor()
        lastrowid = None
        stripped = translated.strip()
        if stripped.upper().startswith("INSERT INTO") and "ON CONFLICT" not in stripped.upper() and "RETURNING" not in stripped.upper():
            translated = stripped.rstrip(';') + " RETURNING id"
            cur.execute(translated, params)
            try:
                row = cur.fetchone()
                if row and "id" in row:
                    lastrowid = row["id"]
            except Exception:
                lastrowid = None
            return PgCursorCompat(cur, lastrowid=lastrowid)
        if "INSERT INTO" in stripped.upper() and "ON CONFLICT DO NOTHING" not in stripped.upper() and "OR IGNORE" not in str(sql).upper():
            cur.execute(translated, params)
        else:
            # Compatibilidade com INSERT OR IGNORE do SQLite.
            if "INSERT INTO" in stripped.upper() and "ON CONFLICT" not in stripped.upper():
                translated = stripped.rstrip(';') + " ON CONFLICT DO NOTHING"
            cur.execute(translated, params)
        return PgCursorCompat(cur)

    def executescript(self, sql_script):
        cur = self._conn.cursor()
        for statement in str(sql_script).split(';'):
            statement = statement.strip()
            if not statement or statement.upper().startswith('PRAGMA'):
                continue
            translated = self._translate_sql(statement)
            if translated:
                cur.execute(translated)
        return PgCursorCompat(cur)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def get_conn():
    if USE_POSTGRES:
        return PgConnCompat()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn



def init_db_postgres():
    """Cria/atualiza a estrutura básica no PostgreSQL."""
    schema_path = Path(__file__).with_name("schema_postgres.sql")
    if not schema_path.exists():
        raise FileNotFoundError("schema_postgres.sql não encontrado na pasta do app.")
    with get_conn() as conn:
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        user_count = conn.execute("SELECT COUNT(*) AS total FROM system_users").fetchone()["total"]
        if user_count == 0:
            conn.execute(
                """
                INSERT INTO system_users (name, username, email, password_hash, profile, active, must_change_password, visual_theme)
                VALUES (%s, %s, %s, %s, %s, 1, 1, 'Tactical Couple')
                """,
                ("Administrador", "admin", None, hash_password("admin123"), "Administrador"),
            )
        conn.commit()


@st.cache_resource(show_spinner=False)
def _init_db_postgres_cached():
    init_db_postgres()
    return True


def init_db():
    if USE_POSTGRES:
        _init_db_postgres_cached()
        return
    with get_conn() as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT,
                barcode TEXT,
                ncm TEXT,
                cest TEXT,
                name TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL DEFAULT 'Outro',
                unit TEXT NOT NULL DEFAULT 'un',
                stock_qty REAL NOT NULL DEFAULT 0,
                min_stock REAL NOT NULL DEFAULT 0,
                cost_unit REAL NOT NULL DEFAULT 0,
                sale_price REAL NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS stock_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                qty REAL NOT NULL,
                unit_cost REAL NOT NULL,
                total_cost REAL NOT NULL,
                supplier TEXT,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(product_id) REFERENCES products(id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_date TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Aberto',
                players INTEGER NOT NULL DEFAULT 0,
                rental_qty INTEGER NOT NULL DEFAULT 0,
                rental_unit_price REAL NOT NULL DEFAULT 0,
                own_equipment_qty INTEGER NOT NULL DEFAULT 0,
                own_equipment_unit_price REAL NOT NULL DEFAULT 0,
                entry_revenue REAL NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_date TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                event_id INTEGER,
                qty REAL NOT NULL,
                unit_price REAL NOT NULL,
                revenue REAL NOT NULL,
                cost_unit_at_sale REAL NOT NULL,
                cogs REAL NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(product_id) REFERENCES products(id),
                FOREIGN KEY(event_id) REFERENCES events(id)
            );

            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                expense_date TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                event_id INTEGER,
                operator_id INTEGER,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(event_id) REFERENCES events(id)
            );

            CREATE TABLE IF NOT EXISTS stock_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movement_date TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                event_id INTEGER,
                movement_type TEXT NOT NULL,
                qty REAL NOT NULL,
                unit_cost REAL NOT NULL DEFAULT 0,
                unit_price REAL NOT NULL DEFAULT 0,
                total_cost REAL NOT NULL DEFAULT 0,
                total_revenue REAL NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(product_id) REFERENCES products(id),
                FOREIGN KEY(event_id) REFERENCES events(id)
            );

            CREATE TABLE IF NOT EXISTS operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                residence TEXT,
                team TEXT,
                phone TEXT,
                cpf TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                number INTEGER NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'Aberta',
                event_id INTEGER,
                operator_id INTEGER,
                customer_name TEXT,
                entry_type TEXT NOT NULL DEFAULT 'Sem entrada',
                entry_value REAL NOT NULL DEFAULT 0,
                entry_original_value REAL NOT NULL DEFAULT 0,
                entry_courtesy INTEGER NOT NULL DEFAULT 0,
                entry_courtesy_reason TEXT,
                discount_percent REAL NOT NULL DEFAULT 0,
                discount_amount REAL NOT NULL DEFAULT 0,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(event_id) REFERENCES events(id),
                FOREIGN KEY(operator_id) REFERENCES operators(id)
            );


            CREATE TABLE IF NOT EXISTS cash_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL DEFAULT 'Aberto',
                opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                opening_amount REAL NOT NULL DEFAULT 0,
                expected_amount REAL NOT NULL DEFAULT 0,
                closing_amount REAL NOT NULL DEFAULT 0,
                difference_amount REAL NOT NULL DEFAULT 0,
                opened_by INTEGER,
                closed_by INTEGER,
                notes TEXT,
                FOREIGN KEY(opened_by) REFERENCES system_users(id),
                FOREIGN KEY(closed_by) REFERENCES system_users(id)
            );

            CREATE TABLE IF NOT EXISTS cash_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                movement_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                movement_type TEXT NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                payment_method TEXT NOT NULL DEFAULT 'Dinheiro',
                command_id INTEGER,
                created_by INTEGER,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(session_id) REFERENCES cash_sessions(id),
                FOREIGN KEY(command_id) REFERENCES commands(id),
                FOREIGN KEY(created_by) REFERENCES system_users(id)
            );

            CREATE TABLE IF NOT EXISTS system_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                email TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                profile TEXT NOT NULL DEFAULT 'Consulta',
                active INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 0,
                last_login TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES system_users(id)
            );
            """
        )
        existing_product_cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
        if "sku" not in existing_product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN sku TEXT")
        if "active" not in existing_product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
        if "barcode" not in existing_product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN barcode TEXT")
        if "ncm" not in existing_product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN ncm TEXT")
        if "cest" not in existing_product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN cest TEXT")
        existing_event_cols = [r[1] for r in conn.execute("PRAGMA table_info(events)").fetchall()]
        event_new_cols = {
            "status": "TEXT NOT NULL DEFAULT 'Aberto'",
            "rental_qty": "INTEGER NOT NULL DEFAULT 0",
            "rental_unit_price": "REAL NOT NULL DEFAULT 0",
            "own_equipment_qty": "INTEGER NOT NULL DEFAULT 0",
            "own_equipment_unit_price": "REAL NOT NULL DEFAULT 0",
        }
        for col, definition in event_new_cols.items():
            if col not in existing_event_cols:
                conn.execute(f"ALTER TABLE events ADD COLUMN {col} {definition}")

        existing_sales_cols = [r[1] for r in conn.execute("PRAGMA table_info(sales)").fetchall()]
        if "command_id" not in existing_sales_cols:
            conn.execute("ALTER TABLE sales ADD COLUMN command_id INTEGER")
        if "operator_id" not in existing_sales_cols:
            conn.execute("ALTER TABLE sales ADD COLUMN operator_id INTEGER")

        existing_operator_cols = [r[1] for r in conn.execute("PRAGMA table_info(operators)").fetchall()]
        if "residence" not in existing_operator_cols:
            conn.execute("ALTER TABLE operators ADD COLUMN residence TEXT")
        if "team" not in existing_operator_cols:
            conn.execute("ALTER TABLE operators ADD COLUMN team TEXT")
        if "phone" not in existing_operator_cols:
            conn.execute("ALTER TABLE operators ADD COLUMN phone TEXT")
        if "cpf" in existing_operator_cols:
            conn.execute("UPDATE operators SET cpf = NULL WHERE cpf IS NOT NULL")

        existing_command_cols = [r[1] for r in conn.execute("PRAGMA table_info(commands)").fetchall()]
        if "entry_type" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN entry_type TEXT NOT NULL DEFAULT 'Sem entrada'")
        if "entry_value" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN entry_value REAL NOT NULL DEFAULT 0")
        if "entry_original_value" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN entry_original_value REAL NOT NULL DEFAULT 0")
            conn.execute("UPDATE commands SET entry_original_value = COALESCE(entry_value, 0) WHERE entry_original_value = 0")
        if "entry_courtesy" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN entry_courtesy INTEGER NOT NULL DEFAULT 0")
        if "entry_courtesy_reason" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN entry_courtesy_reason TEXT")
        if "discount_percent" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN discount_percent REAL NOT NULL DEFAULT 0")
        if "discount_amount" not in existing_command_cols:
            conn.execute("ALTER TABLE commands ADD COLUMN discount_amount REAL NOT NULL DEFAULT 0")

        existing_expense_cols = [r[1] for r in conn.execute("PRAGMA table_info(expenses)").fetchall()]
        if "operator_id" not in existing_expense_cols:
            conn.execute("ALTER TABLE expenses ADD COLUMN operator_id INTEGER")

        existing_user_cols = [r[1] for r in conn.execute("PRAGMA table_info(system_users)").fetchall()]
        if "visual_theme" not in existing_user_cols:
            conn.execute("ALTER TABLE system_users ADD COLUMN visual_theme TEXT NOT NULL DEFAULT 'Tactical Couple'")

        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_sku_unique ON products(sku) WHERE sku IS NOT NULL AND TRIM(sku) <> ''")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique ON system_users(email) WHERE email IS NOT NULL AND TRIM(email) <> ''")

        user_count = conn.execute("SELECT COUNT(*) AS total FROM system_users").fetchone()["total"]
        if user_count == 0:
            conn.execute(
                """
                INSERT INTO system_users (name, username, email, password_hash, profile, active, must_change_password, visual_theme)
                VALUES (?, ?, ?, ?, ?, 1, 1, 'Tactical Couple')
                """,
                ("Administrador", "admin", None, hash_password("admin123"), "Administrador"),
            )
        conn.commit()


def hash_password(password):
    """Gera hash seguro para senha usando PBKDF2-HMAC-SHA256."""
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}:{digest.hex()}"


def verify_password(password, stored_hash):
    try:
        salt_hex, digest_hex = stored_hash.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
        return hmac.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False


def current_user():
    return st.session_state.get("auth_user")


def is_admin():
    user = current_user() or {}
    return user.get("profile") == "Administrador"


def can_export_data():
    user = current_user() or {}
    return user.get("profile") in ["Administrador", "Financeiro"]


def can_delete_system_data():
    return is_admin()


def debug_min_stock_log(message: str):
    """Registra diagnóstico local do popup de estoque mínimo."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user = current_user() or {}
        username = user.get("username", "sem_usuario")
        with DEBUG_MIN_STOCK_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] usuario={username} | {message}\n")
    except Exception:
        pass


def log_action(action, details=""):
    user = current_user() or {}
    try:
        execute("INSERT INTO system_logs (user_id, action, details) VALUES (?, ?, ?)", (user.get("id"), action, details))
    except Exception:
        pass


def login_screen():
    login_logo = LOGO_LOGIN_PATH if LOGO_LOGIN_PATH.exists() else (LOGO_TRANSPARENT_PATH if LOGO_TRANSPARENT_PATH.exists() else LOGO_PATH)
    logo_b64 = image_to_base64(login_logo)
    mime = "png" if str(login_logo).lower().endswith(".png") else "jpeg"
    img_html = f'<img class="login-logo" src="data:image/{mime};base64,{logo_b64}" alt="Subaquáticos">' if logo_b64 else ""
    st.markdown(
        f"""
        <div class="login-shell">
            {img_html}
            <div class="login-title">Tactical</div>
            <div class="login-subtitle">A gestão tática do seu campo.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _, login_col, _ = st.columns([1.2, 1.0, 1.2])
    with login_col:
        with st.form("login_form"):
            username = st.text_input("Usuário ou e-mail", placeholder="Digite seu usuário")
            password = st.text_input("Senha", type="password", placeholder="Digite sua senha")
            submitted = st.form_submit_button("Entrar", width="stretch")

        st.markdown(
            """
            <div style="margin-top:18px; color:#7b674f; font-size:.88rem;">
                Primeiro acesso: <strong>admin</strong> / <strong>admin123</strong>
            </div>
            <div style="margin-top:26px; color:#9b896f; font-size:.78rem;">
                © 2026 Nox Sistemas. Todos os direitos reservados.
            </div>
            """,
            unsafe_allow_html=True,
        )

    if submitted:
        username_clean = username.strip()
        if not username_clean or not password:
            st.error("Informe usuário/e-mail e senha.")
            return
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM system_users
                WHERE active = 1
                  AND (LOWER(username) = LOWER(?) OR LOWER(COALESCE(email, '')) = LOWER(?))
                """,
                (username_clean, username_clean),
            ).fetchone()
            if row and verify_password(password, row["password_hash"]):
                conn.execute("UPDATE system_users SET last_login = CURRENT_TIMESTAMP WHERE id = ?", (row["id"],))
                conn.commit()
                st.session_state["auth_user"] = {
                    "id": row["id"],
                    "name": row["name"],
                    "username": row["username"],
                    "profile": row["profile"],
                    "must_change_password": row["must_change_password"],
                    "visual_theme": row["visual_theme"] if "visual_theme" in row.keys() and row["visual_theme"] in TEMAS_VISUAIS else TEMA_PADRAO,
                }
                log_action("login", f"Login realizado pelo usuário {row['username']}")
                st.success("Login realizado com sucesso.")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos, ou usuário inativo.")


def require_admin():
    if not is_admin():
        st.error("Acesso permitido apenas para Administrador.")
        st.stop()


@st.cache_data(ttl=45, show_spinner=False)
def _query_df_cached(sql, params_tuple, use_postgres):
    params_tuple = tuple(params_tuple or ())
    with get_conn() as conn:
        if use_postgres:
            translated = conn._translate_sql(sql)
            if translated is None:
                return pd.DataFrame()
            cur = conn._conn.cursor()
            try:
                cur.execute(translated, params_tuple)
                rows = cur.fetchall()
                if rows and isinstance(rows[0], dict):
                    return pd.DataFrame([dict(r) for r in rows])
                columns = [desc[0] for desc in (cur.description or [])]
                return pd.DataFrame(rows, columns=columns)
            except Exception as exc:
                try:
                    conn._conn.rollback()
                except Exception:
                    pass
                print("Tactical Cloud SQL error:", repr(exc))
                print("SQL:", translated[:1200])
                print("PARAMS:", params_tuple)
                return pd.DataFrame()
        return pd.read_sql_query(sql, conn, params=params_tuple)


def query_df(sql, params=()):
    return _query_df_cached(str(sql), tuple(params or ()), USE_POSTGRES).copy()


def execute(sql, params=()):
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        try:
            st.cache_data.clear()
        except Exception:
            pass
        return getattr(cur, "lastrowid", None)


def record_stock_movement(conn, movement_date, product_id, event_id, movement_type, qty, unit_cost=0, unit_price=0, notes=""):
    total_cost = float(qty) * float(unit_cost)
    total_revenue = float(qty) * float(unit_price)
    conn.execute(
        """
        INSERT INTO stock_movements (movement_date, product_id, event_id, movement_type, qty, unit_cost, unit_price, total_cost, total_revenue, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (str(movement_date), product_id, event_id, movement_type, qty, unit_cost, unit_price, total_cost, total_revenue, notes),
    )


def add_stock_adjustment(movement_date, product_id, event_id, movement_type, qty, unit_cost, notes):
    """Movimentações manuais. Qty positivo entra no estoque; qty negativo sai do estoque."""
    product_name = "-"
    old_stock = 0.0
    new_stock = 0.0
    cost_to_use = 0.0
    with get_conn() as conn:
        prod = conn.execute("SELECT name, sku, stock_qty, cost_unit FROM products WHERE id = ?", (product_id,)).fetchone()
        if not prod:
            raise ValueError("Produto não encontrado.")
        product_name = prod["name"]
        sku = prod["sku"] or "sem SKU"
        stock = float(prod["stock_qty"])
        old_stock = stock
        if qty < 0 and abs(qty) > stock:
            raise ValueError(f"Estoque insuficiente. Disponível: {stock:g}")
        cost_to_use = float(unit_cost) if float(unit_cost) > 0 else float(prod["cost_unit"])
        new_stock = stock + float(qty)
        conn.execute("UPDATE products SET stock_qty = ? WHERE id = ?", (new_stock, product_id))
        record_stock_movement(conn, movement_date, product_id, event_id, movement_type, qty, cost_to_use, 0, notes)
        conn.commit()
    log_action(
        "movimentacao_estoque_registrada",
        f"Movimentação de estoque registrada | Produto: {product_name} ({sku}) | Tipo: {movement_type} | Quantidade: {float(qty):g} | Estoque anterior: {old_stock:g} | Estoque atual: {new_stock:g} | Custo usado: {brl(cost_to_use)} | Observações: {notes or '-'}",
    )



def expense_type_sql(alias=""):
    prefix = f"{alias}." if alias else ""
    return f"""
        CASE
            WHEN UPPER(COALESCE({prefix}notes, '')) LIKE '%TIPO_DESPESA=FIXA%' THEN 'Fixa'
            WHEN UPPER(COALESCE({prefix}notes, '')) LIKE '%TIPO_DESPESA=VARIAVEL%' THEN 'Variável'
            WHEN LOWER(COALESCE({prefix}category, '')) IN ('aluguel', 'funcionário', 'funcionario') THEN 'Fixa'
            ELSE 'Variável'
        END
    """


def get_event_financials(event_id):
    """Retorna totais financeiros consolidados de um jogo/evento."""
    sales_event = query_df("SELECT COALESCE(SUM(revenue),0) receita, COALESCE(SUM(cogs),0) custo FROM sales WHERE event_id = ?", (event_id,))
    tipo_expr = expense_type_sql()
    expenses_event = query_df(
        f"""
        SELECT
            COALESCE(SUM(amount),0) despesas,
            COALESCE(SUM(CASE WHEN {tipo_expr} = 'Fixa' THEN amount ELSE 0 END),0) despesas_fixas,
            COALESCE(SUM(CASE WHEN {tipo_expr} = 'Variável' THEN amount ELSE 0 END),0) despesas_variaveis
        FROM expenses
        WHERE event_id = ?
        """,
        (event_id,),
    )
    discounts_event = query_df("SELECT COALESCE(SUM(discount_amount),0) desconto FROM commands WHERE event_id = ? AND status = 'Fechada'", (event_id,))
    return {
        "descontos_comandas": float(discounts_event["desconto"].iloc[0]),
        "receita_produtos": float(sales_event["receita"].iloc[0]),
        "custo_produtos": float(sales_event["custo"].iloc[0]),
        "despesas": float(expenses_event["despesas"].iloc[0]),
        "despesas_fixas": float(expenses_event["despesas_fixas"].iloc[0]),
        "despesas_variaveis": float(expenses_event["despesas_variaveis"].iloc[0]),
    }


def delete_expense(expense_id):
    execute("DELETE FROM expenses WHERE id = ?", (int(expense_id),))


def update_expense(expense_id, category, description, amount, expense_type, operator_id=None):
    if float(amount) <= 0:
        raise ValueError("O valor da despesa precisa ser maior que zero.")
    expense_type_clean = "Fixa" if str(expense_type).lower().startswith("fix") else "Variável"
    notes = f"TIPO_DESPESA={expense_type_clean.upper().replace('Á', 'A')}; Editado em Jogos"
    execute(
        """
        UPDATE expenses
        SET category = ?, description = ?, amount = ?, operator_id = ?, notes = ?
        WHERE id = ?
        """,
        (category, description.strip() or "Despesa do jogo", float(amount), operator_id, notes, int(expense_id)),
    )


def event_linked_counts(event_id):
    """Conta registros vinculados ao jogo para evitar exclusão acidental com movimentações."""
    counts = {}
    counts["comandas"] = int(query_df("SELECT COUNT(*) AS total FROM commands WHERE event_id = ?", (event_id,))["total"].iloc[0])
    counts["vendas"] = int(query_df("SELECT COUNT(*) AS total FROM sales WHERE event_id = ?", (event_id,))["total"].iloc[0])
    counts["despesas"] = int(query_df("SELECT COUNT(*) AS total FROM expenses WHERE event_id = ?", (event_id,))["total"].iloc[0])
    counts["movimentações"] = int(query_df("SELECT COUNT(*) AS total FROM stock_movements WHERE event_id = ?", (event_id,))["total"].iloc[0])
    return counts


def delete_event_if_empty(event_id):
    counts = event_linked_counts(event_id)
    total = sum(counts.values())
    if total > 0:
        return False, counts
    execute("DELETE FROM events WHERE id = ?", (int(event_id),))
    return True, counts


def count_open_events():
    df = query_df("SELECT COUNT(*) AS total FROM events WHERE COALESCE(status, 'Aberto') = 'Aberto'")
    return int(df['total'].iloc[0]) if not df.empty else 0


def get_open_events_df():
    return query_df("SELECT id, event_date, name, status FROM events WHERE COALESCE(status, 'Aberto') = 'Aberto' ORDER BY event_date DESC, id DESC")


def finalize_event(event_id):
    open_commands = int(query_df("SELECT COUNT(*) AS total FROM commands WHERE event_id = ? AND status = 'Aberta'", (event_id,))["total"].iloc[0])
    if open_commands > 0:
        raise ValueError(f"Este jogo ainda possui {open_commands} comanda(s) aberta(s). Feche ou cancele as comandas antes de finalizar o jogo.")
    event_info = query_df("SELECT event_date, name FROM events WHERE id = ?", (int(event_id),))
    execute("UPDATE events SET status = 'Fechado' WHERE id = ?", (int(event_id),))
    if not event_info.empty:
        ev = event_info.iloc[0]
        log_action("finalizou_jogo", f"Jogo finalizado: {ev['name']} | Data: {format_date_br(ev['event_date'])} | ID: {event_id}")
    else:
        log_action("finalizou_jogo", f"Jogo ID {event_id} finalizado")


def show_finalize_event_dialog(event_id):
    @st.dialog("Confirmar finalização do jogo")
    def _dialog():
        st.warning("Tem certeza que deseja finalizar este jogo? Após finalizar, ele deixará de ser considerado jogo aberto para novas comandas.")
        open_commands = int(query_df("SELECT COUNT(*) AS total FROM commands WHERE event_id = ? AND status = 'Aberta'", (event_id,))["total"].iloc[0])
        if open_commands > 0:
            st.error(f"Este jogo ainda possui {open_commands} comanda(s) aberta(s). Feche ou cancele as comandas antes de finalizar.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Cancelar", key=f"cancel_finalize_event_popup_{event_id}", width="stretch"):
                st.rerun()
        with col2:
            if st.button("Sim, finalizar", key=f"confirm_finalize_event_popup_{event_id}", type="primary", width="stretch", disabled=open_commands > 0):
                try:
                    finalize_event(event_id)
                    st.session_state["flash_jogo_criado"] = "Jogo finalizado com sucesso. Agora você pode criar um novo jogo."
                    st.session_state["events_tab_choice_next"] = "3. Histórico e resultados"
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
    _dialog()


def show_delete_event_dialog(event_id):
    """Popup de confirmação para excluir jogo criado errado."""
    @st.dialog("Confirmar exclusão do jogo")
    def _dialog():
        st.warning("Tem certeza que deseja excluir este jogo? Essa ação não poderá ser desfeita.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Cancelar", key=f"cancel_delete_event_popup_{event_id}", width="stretch"):
                st.rerun()
        with col2:
            if st.button("Sim, excluir", key=f"confirm_delete_event_popup_{event_id}", type="primary", width="stretch"):
                deleted, counts = delete_event_if_empty(event_id)
                if deleted:
                    st.session_state["flash_jogo_criado"] = "Jogo excluído com sucesso."
                    st.session_state["events_tab_choice_next"] = "1. Criar jogo"
                    st.rerun()
                else:
                    detalhes = ", ".join([f"{nome}: {qtd}" for nome, qtd in counts.items() if qtd > 0])
                    st.error(f"Não foi possível excluir este jogo porque ele já possui lançamentos vinculados ({detalhes}). Exclua ou ajuste esses lançamentos antes.")
    _dialog()

def update_event_totals(event_id, rental_qty, rental_unit_price, own_equipment_qty, own_equipment_unit_price):
    players = int(rental_qty) + int(own_equipment_qty)
    entry_revenue = float(rental_qty) * float(rental_unit_price) + float(own_equipment_qty) * float(own_equipment_unit_price)
    execute(
        """
        UPDATE events
        SET players = ?, rental_qty = ?, rental_unit_price = ?, own_equipment_qty = ?, own_equipment_unit_price = ?, entry_revenue = ?
        WHERE id = ?
        """,
        (players, int(rental_qty), float(rental_unit_price), int(own_equipment_qty), float(own_equipment_unit_price), entry_revenue, event_id),
    )


def product_options(active_only=True):
    where = "WHERE active = 1" if active_only else ""
    df = query_df(f"SELECT id, sku, name, category, stock_qty, cost_unit, sale_price FROM products {where} ORDER BY name")
    return df


def event_options():
    return query_df("SELECT id, event_date, name FROM events ORDER BY event_date DESC, id DESC")


def add_stock_entry(entry_date, product_id, qty, unit_cost, supplier, notes, payment_method="Não informado"):
    total_cost = float(qty) * float(unit_cost)
    if float(qty) <= 0:
        raise ValueError("A quantidade comprada precisa ser maior que zero.")
    if float(unit_cost) < 0:
        raise ValueError("O custo unitário não pode ser negativo.")

    with get_conn() as conn:
        prod = conn.execute("SELECT name, sku, stock_qty, cost_unit FROM products WHERE id = ?", (product_id,)).fetchone()
        if not prod:
            raise ValueError("Produto não encontrado.")

        old_stock = float(prod["stock_qty"])
        old_cost = float(prod["cost_unit"])
        new_stock = old_stock + float(qty)
        new_avg_cost = ((old_stock * old_cost) + total_cost) / new_stock if new_stock > 0 else float(unit_cost)

        cur = conn.execute(
            """
            INSERT INTO stock_entries (entry_date, product_id, qty, unit_cost, total_cost, supplier, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(entry_date), product_id, float(qty), float(unit_cost), total_cost, supplier, notes),
        )
        stock_entry_id = int(cur.lastrowid)

        conn.execute(
            "UPDATE products SET stock_qty = ?, cost_unit = ? WHERE id = ?",
            (new_stock, new_avg_cost, product_id),
        )

        movement_notes = f"Compra de produtos | Fornecedor: {supplier or '-'} | Pagamento: {payment_method or 'Não informado'}"
        if notes:
            movement_notes += f" | Obs: {notes}"
        record_stock_movement(conn, entry_date, product_id, None, "Compra de produtos", float(qty), float(unit_cost), 0, movement_notes)

        expense_notes = (
            f"AUTO_COMPRA_ESTOQUE=1; STOCK_ENTRY_ID={stock_entry_id}; PRODUCT_ID={product_id}; "
            f"SKU={prod['sku'] or '-'}; QTD={float(qty):g}; CUSTO_UNITARIO={float(unit_cost):.2f}; "
            f"FORMA_PAGAMENTO={payment_method or 'Não informado'}; FORNECEDOR={supplier or '-'}"
        )
        if notes:
            expense_notes += f"; OBS={notes}"
        conn.execute(
            """
            INSERT INTO expenses (expense_date, category, description, amount, event_id, operator_id, notes)
            VALUES (?, ?, ?, ?, NULL, NULL, ?)
            """,
            (
                str(entry_date),
                "Compra de produtos",
                f"Compra de estoque - {prod['name']}",
                total_cost,
                expense_notes,
            ),
        )
        conn.commit()

    log_action(
        "compra_estoque_registrada",
        f"Compra de estoque registrada | Produto: {prod['name']} | Quantidade: {float(qty):g} | Total: {brl(total_cost)} | Pagamento: {payment_method or 'Não informado'}",
    )


def add_sale(sale_date, product_id, event_id, qty, unit_price, notes, command_id=None, operator_id=None):
    log_details = None
    with get_conn() as conn:
        prod = conn.execute("SELECT name, sku, stock_qty, cost_unit FROM products WHERE id = ?", (product_id,)).fetchone()
        if not prod:
            raise ValueError("Produto não encontrado.")

        stock = float(prod["stock_qty"])
        qty = float(qty)
        unit_price = float(unit_price)
        if qty > stock:
            raise ValueError(f"Estoque insuficiente. Disponível: {stock:g}")

        cost_unit = float(prod["cost_unit"])
        revenue = qty * unit_price
        cogs = qty * cost_unit

        merged_existing_item = False
        if command_id:
            existing_item = conn.execute(
                """
                SELECT id, qty, revenue, cogs, notes
                FROM sales
                WHERE command_id = ? AND product_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (int(command_id), int(product_id)),
            ).fetchone()
        else:
            existing_item = None

        if existing_item:
            merged_existing_item = True
            old_qty = float(existing_item["qty"] or 0)
            old_revenue = float(existing_item["revenue"] or 0)
            old_cogs = float(existing_item["cogs"] or 0)
            new_qty = old_qty + qty
            new_revenue = old_revenue + revenue
            new_cogs = old_cogs + cogs
            new_unit_price = new_revenue / new_qty if new_qty else unit_price
            merged_notes = existing_item["notes"] or ""
            if notes and notes not in merged_notes:
                merged_notes = (merged_notes + " | " if merged_notes else "") + str(notes)
            conn.execute(
                """
                UPDATE sales
                SET qty = ?, unit_price = ?, revenue = ?, cogs = ?, notes = ?
                WHERE id = ?
                """,
                (new_qty, new_unit_price, new_revenue, new_cogs, merged_notes, int(existing_item["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO sales (sale_date, product_id, event_id, command_id, operator_id, qty, unit_price, revenue, cost_unit_at_sale, cogs, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (str(sale_date), product_id, event_id, command_id, operator_id, qty, unit_price, revenue, cost_unit, cogs, notes),
            )

        conn.execute("UPDATE products SET stock_qty = stock_qty - ? WHERE id = ?", (qty, product_id))
        record_stock_movement(conn, sale_date, product_id, event_id, "Venda/baixa", -float(qty), cost_unit, unit_price, notes)
        if command_id:
            cmd = conn.execute(
                """
                SELECT c.number, COALESCE(o.name, '-') AS operator_name, COALESCE(e.name, '-') AS event_name
                FROM commands c
                LEFT JOIN operators o ON o.id = c.operator_id
                LEFT JOIN events e ON e.id = c.event_id
                WHERE c.id = ?
                """,
                (int(command_id),),
            ).fetchone()
            command_number = int(cmd["number"]) if cmd else int(command_id)
            operator_name = cmd["operator_name"] if cmd else "-"
            event_name = cmd["event_name"] if cmd else "-"
            product_name = prod["name"]
            sku = prod["sku"] or "sem SKU"
            merge_txt = " | Item somado ao card existente" if merged_existing_item else ""
            log_details = (
                f"Comanda #{command_number} | Jogo: {event_name} | Operador/jogador: {operator_name} | "
                f"Produto: {product_name} ({sku}) | Qtd adicionada: {qty:g} | Preço unitário: {brl(unit_price)} | Total adicionado: {brl(revenue)}{merge_txt}"
            )
        conn.commit()
    if log_details:
        log_action("venda_adicionada_comanda", log_details)


def get_sale_for_command_edit(sale_id):
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT s.*, c.number AS command_number, c.status AS command_status
            FROM sales s
            LEFT JOIN commands c ON c.id = s.command_id
            WHERE s.id = ?
            """,
            (int(sale_id),),
        ).fetchone()


def update_command_sale(sale_id, new_product_id, new_qty, new_unit_price, notes=""):
    """Edita um item lançado em comanda aberta e ajusta o estoque automaticamente."""
    new_qty = float(new_qty)
    new_unit_price = float(new_unit_price)
    if new_qty <= 0:
        raise ValueError("A quantidade precisa ser maior que zero.")
    if new_unit_price < 0:
        raise ValueError("O preço unitário não pode ser negativo.")

    with get_conn() as conn:
        sale = conn.execute(
            """
            SELECT s.*, c.number AS command_number, c.status AS command_status
            FROM sales s
            LEFT JOIN commands c ON c.id = s.command_id
            WHERE s.id = ?
            """,
            (int(sale_id),),
        ).fetchone()
        if not sale:
            raise ValueError("Item não encontrado.")
        if sale["command_id"] is None:
            raise ValueError("Este item não pertence a uma comanda.")
        if sale["command_status"] != "Aberta":
            raise ValueError("Só é possível editar itens de comandas abertas.")

        old_product_id = int(sale["product_id"])
        old_qty = float(sale["qty"])
        old_cost_unit = float(sale["cost_unit_at_sale"] or 0)
        event_id = sale["event_id"]
        command_number = int(sale["command_number"] or 0)

        # Devolve o item antigo ao estoque antes de aplicar a nova baixa.
        conn.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?", (old_qty, old_product_id))
        record_stock_movement(
            conn,
            date.today(),
            old_product_id,
            event_id,
            "Estorno por edição de comanda",
            old_qty,
            old_cost_unit,
            float(sale["unit_price"] or 0),
            f"Edição do item {sale_id} da comanda #{command_number}",
        )

        new_product = conn.execute("SELECT stock_qty, cost_unit FROM products WHERE id = ?", (int(new_product_id),)).fetchone()
        if not new_product:
            raise ValueError("Produto novo não encontrado.")
        available = float(new_product["stock_qty"] or 0)
        if new_qty > available:
            raise ValueError(f"Estoque insuficiente para o novo lançamento. Disponível: {available:g}")

        new_cost_unit = float(new_product["cost_unit"] or 0)
        new_revenue = new_qty * new_unit_price
        new_cogs = new_qty * new_cost_unit
        conn.execute(
            """
            UPDATE sales
            SET product_id = ?, qty = ?, unit_price = ?, revenue = ?, cost_unit_at_sale = ?, cogs = ?, notes = ?
            WHERE id = ?
            """,
            (int(new_product_id), new_qty, new_unit_price, new_revenue, new_cost_unit, new_cogs, notes, int(sale_id)),
        )
        conn.execute("UPDATE products SET stock_qty = stock_qty - ? WHERE id = ?", (new_qty, int(new_product_id)))
        record_stock_movement(
            conn,
            date.today(),
            int(new_product_id),
            event_id,
            "Baixa por edição de comanda",
            -new_qty,
            new_cost_unit,
            new_unit_price,
            f"Edição do item {sale_id} da comanda #{command_number}",
        )
        conn.commit()


def delete_command_sale(sale_id):
    """Exclui um item de uma comanda aberta e devolve a quantidade ao estoque."""
    with get_conn() as conn:
        sale = conn.execute(
            """
            SELECT s.*, c.number AS command_number, c.status AS command_status
            FROM sales s
            LEFT JOIN commands c ON c.id = s.command_id
            WHERE s.id = ?
            """,
            (int(sale_id),),
        ).fetchone()
        if not sale:
            raise ValueError("Item não encontrado.")
        if sale["command_id"] is None:
            raise ValueError("Este item não pertence a uma comanda.")
        if sale["command_status"] != "Aberta":
            raise ValueError("Só é possível excluir itens de comandas abertas.")

        product_id = int(sale["product_id"])
        qty = float(sale["qty"])
        conn.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?", (qty, product_id))
        record_stock_movement(
            conn,
            date.today(),
            product_id,
            sale["event_id"],
            "Estorno de item de comanda",
            qty,
            float(sale["cost_unit_at_sale"] or 0),
            float(sale["unit_price"] or 0),
            f"Exclusão do item {sale_id} da comanda #{int(sale['command_number'] or 0)}",
        )
        conn.execute("DELETE FROM sales WHERE id = ?", (int(sale_id),))
        conn.commit()


def date_filter_sql(column, start_date, end_date):
    return f"DATE({column}) BETWEEN DATE(?) AND DATE(?)", (str(start_date), str(end_date))


def kpis(start_date, end_date):
    period_sql, params = date_filter_sql("event_date", start_date, end_date)
    events = query_df(f"SELECT COALESCE(SUM(entry_revenue), 0) AS total FROM events WHERE {period_sql}", params)["total"].iloc[0]

    period_sql, params = date_filter_sql("sale_date", start_date, end_date)
    sales = query_df(f"SELECT COALESCE(SUM(revenue), 0) AS revenue, COALESCE(SUM(cogs), 0) AS cogs FROM sales WHERE {period_sql}", params)
    revenue_sales = sales["revenue"].iloc[0]
    cogs = sales["cogs"].iloc[0]

    period_sql, params = date_filter_sql("expense_date", start_date, end_date)
    stock_purchase_where = "(category = 'Compra de produtos' OR COALESCE(notes, '') LIKE '%AUTO_COMPRA_ESTOQUE=1%')"
    expenses_df = query_df(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN {stock_purchase_where} THEN amount ELSE 0 END), 0) AS compras_estoque,
            COALESCE(SUM(CASE WHEN NOT {stock_purchase_where} THEN amount ELSE 0 END), 0) AS despesas_operacionais,
            COALESCE(SUM(amount), 0) AS despesas_total
        FROM expenses
        WHERE {period_sql}
        """,
        params,
    )
    stock_purchases = float(expenses_df["compras_estoque"].iloc[0])
    operational_expenses = float(expenses_df["despesas_operacionais"].iloc[0])
    expenses_total = float(expenses_df["despesas_total"].iloc[0])

    inventory_value = query_df("SELECT COALESCE(SUM(stock_qty * cost_unit), 0) AS total FROM products WHERE active = 1")["total"].iloc[0]

    discounts = query_df(
        """
        SELECT COALESCE(SUM(discount_amount), 0) AS total
        FROM commands
        WHERE status = 'Fechada'
          AND DATE(COALESCE(closed_at, opened_at)) BETWEEN DATE(?) AND DATE(?)
        """,
        (str(start_date), str(end_date)),
    )["total"].iloc[0]

    total_revenue = events + revenue_sales - discounts
    gross_profit = total_revenue - cogs
    # Compras de estoque entram como controle de caixa/compra, mas o lucro operacional usa o custo do produto vendido para evitar dupla contagem.
    net_profit = total_revenue - cogs - operational_expenses

    period_sql, params = date_filter_sql("event_date", start_date, end_date)
    players = query_df(f"SELECT COALESCE(SUM(players), 0) AS total FROM events WHERE {period_sql}", params)["total"].iloc[0]

    return {
        "receita_entradas": events,
        "receita_vendas": revenue_sales,
        "descontos": discounts,
        "receita_total": total_revenue,
        "custo_produtos": cogs,
        "despesas": operational_expenses,
        "despesas_operacionais": operational_expenses,
        "compras_estoque": stock_purchases,
        "despesas_total": expenses_total,
        "valor_em_estoque": inventory_value,
        "custos_totais": cogs + operational_expenses + stock_purchases,
        "lucro_bruto": gross_profit,
        "lucro_liquido": net_profit,
        "jogadores": players,
        "ticket_medio": total_revenue / players if players else 0,
        "lucro_por_jogador": net_profit / players if players else 0,
        "margem_liquida": (net_profit / total_revenue * 100) if total_revenue else 0,
        "consumo_medio_por_jogador": revenue_sales / players if players else 0,
    }


def seed_data():
    produtos = [
        ("BEB-AGUA-500", "Água 500ml", "Bebida", "un", 24, 8, 1.20, 4.00),
        ("BEB-REFRI-LATA", "Refrigerante lata", "Bebida", "un", 36, 10, 2.80, 7.00),
        ("BEB-ENERGETICO", "Energético", "Bebida", "un", 12, 6, 5.50, 12.00),
        ("COM-HAMB", "Hambúrguer", "Comida", "un", 20, 8, 8.00, 18.00),
        ("COM-SALG", "Salgado", "Comida", "un", 30, 10, 3.50, 8.00),
        ("EQP-BB-025", "Munição BB 0.25g", "Equipamento", "pacote", 10, 3, 45.00, 70.00),
    ]
    with get_conn() as conn:
        for p in produtos:
            conn.execute(
                """
                INSERT OR IGNORE INTO products (sku, name, category, unit, stock_qty, min_stock, cost_unit, sale_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                p,
            )
        conn.commit()


def seed_operators():
    """Insere uma lista simples de operadores/jogadores para testes."""
    operadores = [
        ("João Silva", "Cotia", "Subaquáticos", "11910000001", None),
        ("Pedro Santos", "Vargem Grande", "Subaquáticos", "11910000002", None),
        ("Lucas Oliveira", "Osasco", "Bravo Team", "11910000003", None),
        ("Gabriel Costa", "Carapicuíba", "Bravo Team", "11910000004", None),
        ("Rafael Almeida", "Barueri", "Alpha Squad", "11910000005", None),
        ("Bruno Pereira", "Jandira", "Alpha Squad", "11910000006", None),
        ("Felipe Souza", "Itapevi", "Ghost Team", "11910000007", None),
        ("Gustavo Lima", "Cotia", "Ghost Team", "11910000008", None),
        ("Carlos Mendes", "São Paulo", "Delta", "11910000009", None),
        ("Diego Martins", "Taboão", "Delta", "11910000010", None),
        ("André Rocha", "Embu", "Rangers", "11910000011", None),
        ("Mateus Ribeiro", "Cotia", "Rangers", "11910000012", None),
        ("Thiago Barbosa", "Osasco", "Falcons", "11910000013", None),
        ("Leonardo Ferreira", "Barueri", "Falcons", "11910000014", None),
        ("Henrique Gomes", "Carapicuíba", "Titans", "11910000015", None),
        ("Victor Nunes", "Jandira", "Titans", "11910000016", None),
        ("Eduardo Castro", "Itapevi", "Warriors", "11910000017", None),
        ("Marcos Teixeira", "Cotia", "Warriors", "11910000018", None),
        ("Daniel Moreira", "São Paulo", "Fox Team", "11910000019", None),
        ("Caio Fernandes", "Vargem Grande", "Fox Team", "11910000020", None),
    ]
    inseridos = 0
    with get_conn() as conn:
        for nome, residencia, equipe, telefone, cpf in operadores:
            existe = conn.execute("SELECT id FROM operators WHERE LOWER(name) = LOWER(?)", (nome,)).fetchone()
            if existe:
                continue
            conn.execute(
                "INSERT INTO operators (name, residence, team, phone, cpf, active) VALUES (?, ?, ?, ?, ?, 1)",
                (nome, residencia, equipe, telefone, cpf),
            )
            inseridos += 1
        conn.commit()
    return inseridos


def suppress_streamlit_enter_tooltips():
    """Mantém o submit com Enter, mas remove o aviso visual 'Press Enter to submit form'."""
    components.html(
        """
        <script>
        (function() {
            const TARGETS = [
                'press enter to submit form',
                'submit form',
                'pressione enter para enviar'
            ];

            function mustRemove(value) {
                const txt = String(value || '').toLowerCase().trim();
                return TARGETS.some(t => txt.includes(t));
            }

            function cleanNode(root) {
                if (!root) return;

                const walker = (node) => {
                    if (!node) return;

                    if (node.nodeType === 1) {
                        try {
                            const title = node.getAttribute && node.getAttribute('title');
                            const aria = node.getAttribute && node.getAttribute('aria-label');
                            const dataTooltip = node.getAttribute && node.getAttribute('data-tooltip');

                            if (mustRemove(title)) node.removeAttribute('title');
                            if (mustRemove(aria)) node.removeAttribute('aria-label');
                            if (mustRemove(dataTooltip)) node.removeAttribute('data-tooltip');

                            const text = (node.textContent || '').trim().toLowerCase();
                            if (
                                text === 'press enter to submit form' ||
                                text === 'pressione enter para enviar'
                            ) {
                                node.style.display = 'none';
                                node.setAttribute('aria-hidden', 'true');
                            }
                        } catch (e) {}

                        try {
                            if (node.shadowRoot) walker(node.shadowRoot);
                        } catch (e) {}

                        try {
                            if (node.tagName === 'IFRAME' && node.contentDocument) {
                                walker(node.contentDocument);
                            }
                        } catch (e) {}
                    }

                    let children = [];
                    try { children = node.children ? Array.from(node.children) : []; } catch (e) {}
                    children.forEach(walker);
                };

                walker(root.documentElement || root.body || root);
            }

            function runClean() {
                try { cleanNode(document); } catch (e) {}
                try { cleanNode(window.parent.document); } catch (e) {}
            }

            runClean();
            const interval = setInterval(runClean, 200);
            setTimeout(function() { clearInterval(interval); }, 30000);

            try {
                const target = window.parent && window.parent.document && window.parent.document.body
                    ? window.parent.document.body
                    : document.body;
                const observer = new MutationObserver(runClean);
                observer.observe(target, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    attributeFilter: ['title', 'aria-label', 'data-tooltip']
                });
            } catch (e) {}
        })();
        </script>
        """,
        height=0,
        width=0,
    )

def render_header():
    st.set_page_config(page_title="Tactical", page_icon="🎯", layout="wide")
    apply_brand_styles()
    suppress_streamlit_enter_tooltips()
    if current_user():
        render_brand_header()


def render_sidebar():
    user = current_user() or {}
    sidebar_logo = LOGO_TRANSPARENT_PATH if LOGO_TRANSPARENT_PATH.exists() else LOGO_PATH
    if sidebar_logo.exists():
        st.sidebar.image(str(sidebar_logo), width="stretch")
    st.sidebar.markdown("### Tactical")
    st.sidebar.caption(f"Logado: **{user.get('name', '')}**")
    st.sidebar.caption(f"Perfil: {user.get('profile', '')}")
    if st.sidebar.button("Sair"):
        log_action("logout", f"Logout realizado pelo usuário {user.get('username', '')}")
        st.session_state.pop("auth_user", None)
        st.rerun()

    if user.get("must_change_password"):
        st.sidebar.warning("Altere a senha padrão em Usuários.")

    menu = MENU_POR_PERFIL.get(user.get("profile"), MENU_POR_PERFIL["Consulta"])
    # Ícones escolhidos a partir da lógica visual de bibliotecas como Lucide/Font Awesome.
    icons = {
        "Dashboard": "📊",      # painel / métricas
        "Jogos": "🎯",          # evento / partida
        "Operadores": "👥",     # jogadores cadastrados
        "Comandas": "🧾",       # comanda / recibo
        "Caixa": "💰",          # abertura e fechamento de caixa
        "Produtos": "📦",       # produto / pacote
        "Estoque": "🏷️",        # controle de estoque
        "Despesas": "💸",       # saída financeira
        "Relatórios": "📈",     # relatórios e gráficos
        "Usuários": "👤",       # usuários do sistema
        "Logs/Auditoria": "🛡️", # auditoria de ações
        "Configurações": "⚙️",  # ajustes
    }
    labels = [f"{icons.get(item, '•')}  {item}" for item in menu]
    forced_page = st.session_state.pop("force_sidebar_page", None)
    if forced_page and forced_page in menu:
        st.session_state["main_menu_choice"] = labels[menu.index(forced_page)]
    if "main_menu_choice" in st.session_state and st.session_state["main_menu_choice"] not in labels:
        st.session_state["main_menu_choice"] = labels[0]
    selected_label = st.sidebar.radio("Ir para", labels, key="main_menu_choice")
    selected_index = labels.index(selected_label)
    st.sidebar.markdown("---")
    st.sidebar.markdown('<div class="sidebar-anchor">⚓</div>', unsafe_allow_html=True)
    st.sidebar.markdown('<div class="sidebar-copyright">© 2026 Nox Sistemas.<br>Todos os direitos reservados.</div>', unsafe_allow_html=True)
    return menu[selected_index]


def render_period_filter():
    today = date.today()
    first_day = today.replace(day=1)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Data inicial", value=first_day, format="DD/MM/YYYY")
    with col2:
        end_date = st.date_input("Data final", value=today, format="DD/MM/YYYY")
    if start_date > end_date:
        st.warning("A data inicial não pode ser maior que a data final. Ajustei o período automaticamente.")
        start_date, end_date = end_date, start_date
    return start_date, end_date



def operators_options(active_only=True):
    where = "WHERE active = 1" if active_only else ""
    return query_df(f"SELECT id, name, residence, team, phone, active FROM operators {where} ORDER BY name")


def command_options(status=None):
    where = ""
    params = ()
    if status:
        where = "WHERE c.status = ?"
        params = (status,)
    return query_df(
        f"""
        SELECT c.id, c.number, c.status, c.opened_at, c.event_id, c.operator_id,
               COALESCE(e.name, '-') AS event_name,
               COALESCE(o.name, '-') AS operator_name,
               COALESCE(c.customer_name, '') AS customer_name,
               COALESCE(c.entry_type, 'Sem entrada') AS entry_type,
               COALESCE(c.entry_value, 0) AS entry_value,
               COALESCE(c.entry_original_value, c.entry_value, 0) AS entry_original_value,
               COALESCE(c.entry_courtesy, 0) AS entry_courtesy,
               COALESCE(c.entry_courtesy_reason, '') AS entry_courtesy_reason,
               COALESCE(c.discount_percent, 0) AS discount_percent,
               COALESCE(c.discount_amount, 0) AS discount_amount,
               COALESCE(c.entry_value, 0) + COALESCE(SUM(s.revenue), 0) AS subtotal,
               COALESCE(c.entry_value, 0) + COALESCE(SUM(s.revenue), 0) - COALESCE(c.discount_amount, 0) AS total
        FROM commands c
        LEFT JOIN events e ON e.id = c.event_id
        LEFT JOIN operators o ON o.id = c.operator_id
        LEFT JOIN sales s ON s.command_id = c.id
        {where}
        GROUP BY c.id, e.name, o.name
        ORDER BY c.number DESC
        """,
        params,
    )


def next_command_number():
    df = query_df("SELECT COALESCE(MAX(number), 99) + 1 AS next_number FROM commands")
    n = int(df["next_number"].iloc[0]) if not df.empty else 100
    return max(n, 100)


def sync_event_from_commands(event_id):
    """Atualiza os totais de jogadores e entradas do jogo com base nas comandas não canceladas."""
    if event_id is None:
        return
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN entry_type = 'Aluguel' AND status <> 'Cancelada' THEN 1 ELSE 0 END), 0) AS rental_qty,
                COALESCE(SUM(CASE WHEN entry_type = 'Equipamento próprio' AND status <> 'Cancelada' THEN 1 ELSE 0 END), 0) AS own_qty,
                COALESCE(SUM(CASE WHEN status <> 'Cancelada' THEN entry_value ELSE 0 END), 0) AS entry_revenue
            FROM commands
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        rental_qty = int(row["rental_qty"] or 0)
        own_qty = int(row["own_qty"] or 0)
        players = rental_qty + own_qty
        entry_revenue = float(row["entry_revenue"] or 0)
        conn.execute(
            """
            UPDATE events
            SET players = ?, rental_qty = ?, own_equipment_qty = ?, entry_revenue = ?
            WHERE id = ?
            """,
            (players, rental_qty, own_qty, entry_revenue, event_id),
        )
        conn.commit()


def existing_command_for_operator(event_id, operator_id):
    """Retorna uma comanda já criada para o operador/jogador no jogo informado.

    Regra de negócio: um operador/jogador pode ter no máximo uma comanda por jogo.
    Comandas canceladas não bloqueiam nova abertura, porque foram anuladas.
    """
    df = query_df(
        """
        SELECT id, number, status
        FROM commands
        WHERE event_id = ?
          AND operator_id = ?
          AND status <> 'Cancelada'
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(event_id), int(operator_id)),
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def create_command(opened_at, event_id, operator_id, customer_name, entry_type, entry_value, notes, entry_original_value=None, entry_courtesy=False, entry_courtesy_reason=""):
    existing = existing_command_for_operator(event_id, operator_id)
    if existing:
        raise ValueError(
            f"Este operador/jogador já possui a comanda #{int(existing['number'])} neste jogo. "
            "Não é permitido abrir mais de uma comanda para o mesmo cadastro no mesmo jogo."
        )

    number = next_command_number()
    original_value = float(entry_value if entry_original_value is None else entry_original_value or 0)
    charged_value = 0.0 if bool(entry_courtesy) else float(entry_value or 0)
    command_id = execute(
        """
        INSERT INTO commands (
            number, status, event_id, operator_id, customer_name, entry_type,
            entry_value, entry_original_value, entry_courtesy, entry_courtesy_reason,
            opened_at, notes
        )
        VALUES (?, 'Aberta', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            number,
            event_id,
            operator_id,
            customer_name.strip() if customer_name else None,
            entry_type,
            charged_value,
            original_value,
            1 if bool(entry_courtesy) else 0,
            entry_courtesy_reason.strip() if entry_courtesy_reason else None,
            str(opened_at),
            notes,
        ),
    )
    sync_event_from_commands(event_id)
    cortesia_txt = f" | Cortesia: sim | Valor original: {brl(original_value)} | Motivo: {entry_courtesy_reason or '-'}" if entry_courtesy else ""
    log_action("criou_comanda", f"Comanda #{number} criada | Tipo de entrada: {entry_type} | Valor cobrado: {brl(charged_value)}{cortesia_txt}")
    return command_id

def close_command(command_id, discount_percent=0.0, payment_method="Dinheiro", entry_courtesy=None, entry_courtesy_reason=""):
    discount_percent = max(0.0, min(float(discount_percent or 0), 100.0))

    cash = get_open_cash_session()
    if cash is None:
        raise ValueError("Abra um caixa antes de fechar comandas. Vá em Caixa > Abrir caixa.")

    with get_conn() as conn:
        if entry_courtesy is not None:
            cmd_entry = conn.execute(
                "SELECT entry_value, entry_original_value FROM commands WHERE id = ?",
                (int(command_id),),
            ).fetchone()
            if cmd_entry:
                current_entry = float(cmd_entry["entry_value"] or 0)
                original_entry = float(cmd_entry["entry_original_value"] or 0)
                if original_entry <= 0:
                    original_entry = current_entry
                new_entry_value = 0.0 if bool(entry_courtesy) else original_entry
                conn.execute(
                    """
                    UPDATE commands
                    SET entry_value = ?, entry_original_value = ?, entry_courtesy = ?, entry_courtesy_reason = ?
                    WHERE id = ?
                    """,
                    (
                        float(new_entry_value),
                        float(original_entry),
                        1 if bool(entry_courtesy) else 0,
                        entry_courtesy_reason.strip() if bool(entry_courtesy) and entry_courtesy_reason else None,
                        int(command_id),
                    ),
                )

        subtotal_row = conn.execute(
            """
            SELECT COALESCE(c.entry_value, 0) + COALESCE(SUM(s.revenue), 0) AS subtotal,
                   c.number AS number,
                   c.event_id AS event_id
            FROM commands c
            LEFT JOIN sales s ON s.command_id = c.id
            WHERE c.id = ?
            GROUP BY c.id
            """,
            (command_id,),
        ).fetchone()
        subtotal = float(subtotal_row["subtotal"]) if subtotal_row else 0.0
        command_number = int(subtotal_row["number"]) if subtotal_row else int(command_id)
        event_id = int(subtotal_row["event_id"]) if subtotal_row and subtotal_row["event_id"] is not None else None
        discount_amount = subtotal * discount_percent / 100.0
        total_final = subtotal - discount_amount

        conn.execute(
            """
            UPDATE commands
            SET status = 'Fechada', closed_at = ?, discount_percent = ?, discount_amount = ?
            WHERE id = ?
            """,
            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), discount_percent, discount_amount, command_id),
        )
        conn.execute(
            """
            INSERT INTO cash_movements (session_id, movement_type, description, amount, payment_method, command_id, created_by, notes)
            VALUES (?, 'Entrada', ?, ?, ?, ?, ?, ?)
            """,
            (
                int(cash["id"]),
                f"Recebimento da comanda #{command_number}",
                float(total_final),
                payment_method,
                int(command_id),
                (current_user() or {}).get("id"),
                f"Subtotal {brl(subtotal)} | Desconto {discount_percent:.2f}% ({brl(discount_amount)})",
            ),
        )
        conn.commit()

    if event_id:
        sync_event_from_commands(event_id)
    courtesy_txt = f" | Entrada em cortesia: {'sim' if entry_courtesy else 'não'}"
    if entry_courtesy:
        courtesy_txt += f" | Motivo: {entry_courtesy_reason or '-'}"
    log_action("fechou_comanda", f"Comanda #{command_number} fechada em {payment_method}: {brl(total_final)}{courtesy_txt}")
    log_action("pagamento_recebido", f"Comanda #{command_number} | Tipo de pagamento: {payment_method} | Valor recebido: {brl(total_final)} | Desconto: {discount_percent:.2f}% ({brl(discount_amount)}){courtesy_txt}")


def reopen_command(command_id):
    execute("UPDATE commands SET status = 'Aberta', closed_at = NULL, discount_percent = 0, discount_amount = 0 WHERE id = ?", (command_id,))


def cancel_command(command_id):
    total = query_df("SELECT COUNT(*) AS qtd FROM sales WHERE command_id = ?", (command_id,))["qtd"].iloc[0]
    cmd = get_command(command_id)
    if int(total) > 0:
        raise ValueError("Não é possível cancelar uma comanda que já possui vendas. Exclua/estorne os lançamentos manualmente antes.")
    execute("UPDATE commands SET status = 'Cancelada', closed_at = ? WHERE id = ?", (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), command_id))
    if cmd is not None and pd.notna(cmd.get('event_id')):
        sync_event_from_commands(int(cmd['event_id']))

def get_command(command_id):
    df = query_df(
        """
        SELECT c.*, COALESCE(e.name, '-') AS event_name, COALESCE(o.name, '-') AS operator_name
        FROM commands c
        LEFT JOIN events e ON e.id = c.event_id
        LEFT JOIN operators o ON o.id = c.operator_id
        WHERE c.id = ?
        """,
        (command_id,),
    )
    return None if df.empty else df.iloc[0]





FORMAS_PAGAMENTO = ["Dinheiro", "Pix", "Cartão de débito", "Cartão de crédito", "Cortesia", "Outro"]


def get_open_cash_session():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM cash_sessions WHERE status = 'Aberto' ORDER BY id DESC LIMIT 1").fetchone()


def cash_expected_amount(session_id):
    with get_conn() as conn:
        session = conn.execute("SELECT opening_amount FROM cash_sessions WHERE id = ?", (session_id,)).fetchone()
        opening = float(session["opening_amount"] or 0) if session else 0.0
        rows = conn.execute(
            """
            SELECT movement_type, COALESCE(SUM(amount), 0) AS total
            FROM cash_movements
            WHERE session_id = ?
            GROUP BY movement_type
            """,
            (session_id,),
        ).fetchall()
    total = opening
    for r in rows:
        tipo = str(r["movement_type"])
        amount = float(r["total"] or 0)
        if tipo == "Saída":
            total -= amount
        else:
            total += amount
    return total


def open_cash(opening_amount, notes=""):
    if float(opening_amount) <= 0:
        raise ValueError("Informe um valor inicial maior que zero para abrir o caixa.")
    if get_open_cash_session() is not None:
        raise ValueError("Já existe um caixa aberto. Feche o caixa atual antes de abrir outro.")
    user_id = (current_user() or {}).get("id")
    cash_id = execute(
        """
        INSERT INTO cash_sessions (status, opening_amount, expected_amount, opened_by, notes)
        VALUES ('Aberto', ?, ?, ?, ?)
        """,
        (float(opening_amount), float(opening_amount), user_id, notes),
    )
    log_action("abriu_caixa", f"Caixa aberto com {brl(opening_amount)}")
    return cash_id


def add_cash_movement(session_id, movement_type, description, amount, payment_method="Dinheiro", notes=""):
    if float(amount) <= 0:
        raise ValueError("Informe um valor maior que zero.")
    user_id = (current_user() or {}).get("id")
    execute(
        """
        INSERT INTO cash_movements (session_id, movement_type, description, amount, payment_method, created_by, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (int(session_id), movement_type, description.strip(), float(amount), payment_method, user_id, notes),
    )
    log_action("movimento_caixa", f"{movement_type}: {description.strip()} - {brl(amount)}")


def close_cash(session_id, closing_amount, notes=""):
    expected = cash_expected_amount(session_id)
    difference = float(closing_amount) - float(expected)
    user_id = (current_user() or {}).get("id")
    execute(
        """
        UPDATE cash_sessions
        SET status = 'Fechado', closed_at = CURRENT_TIMESTAMP, expected_amount = ?, closing_amount = ?, difference_amount = ?, closed_by = ?, notes = COALESCE(notes, '') || ?
        WHERE id = ? AND status = 'Aberto'
        """,
        (float(expected), float(closing_amount), float(difference), user_id, f"\nFechamento: {notes}" if notes else "", int(session_id)),
    )
    log_action("fechou_caixa", f"Caixa fechado. Esperado {brl(expected)}, conferido {brl(closing_amount)}, diferença {brl(difference)}")
    return expected, difference


def page_cash():
    st.subheader("Caixa")
    st.caption("Abra o caixa antes de fechar comandas. Todo fechamento de comanda entra automaticamente no caixa aberto.")

    tab1, tab2 = st.tabs(["Caixa atual", "Histórico de caixas"])

    with tab1:
        open_session = get_open_cash_session()
        if open_session is None:
            st.info("Nenhum caixa aberto no momento.")
            with st.form("form_open_cash", clear_on_submit=True):
                opening_amount_text = st.text_input(
                    "Valor inicial / fundo de caixa",
                    value="",
                    placeholder="Digite o valor. Ex.: 100,00",
                    key="cash_opening_amount_text",
                )
                notes = st.text_area("Observações da abertura")
                if st.form_submit_button("Abrir caixa", type="primary"):
                    opening_amount = parse_money_input(opening_amount_text)
                    if opening_amount <= 0:
                        st.error("Informe um valor inicial maior que zero para abrir o caixa.")
                    else:
                        try:
                            open_cash(opening_amount, notes)
                            st.success("Caixa aberto com sucesso.")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
            return

        session_id = int(open_session["id"])
        expected = cash_expected_amount(session_id)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Status", "Aberto")
        c2.metric("Abertura", brl(open_session["opening_amount"]))
        c3.metric("Saldo esperado", brl(expected))
        c4.metric("Aberto em", format_date_br(open_session["opened_at"]))

        resumo = query_df(
            """
            SELECT payment_method AS Forma_pagamento,
                   SUM(CASE WHEN movement_type = 'Saída' THEN -amount ELSE amount END) AS Valor
            FROM cash_movements
            WHERE session_id = ?
            GROUP BY payment_method
            ORDER BY SUM(CASE WHEN movement_type = 'Saída' THEN -amount ELSE amount END) DESC
            """,
            (session_id,),
        )
        st.write("**Resumo por forma de pagamento**")
        if resumo.empty:
            st.info("Nenhuma movimentação registrada neste caixa.")
        else:
            resumo["Valor"] = resumo["Valor"].map(brl)
            pretty_dataframe(resumo, width="stretch", hide_index=True)

        st.write("**Adicionar movimentação manual**")
        with st.form("form_cash_movement", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            movement_type = c1.selectbox("Tipo", ["Entrada", "Saída"])
            payment_method = c2.selectbox("Forma de pagamento", FORMAS_PAGAMENTO)
            amount = c3.number_input("Valor", min_value=0.0, step=10.0, format="%.2f")
            description = st.text_input("Descrição", placeholder="Ex: Troco, sangria, ajuste de caixa")
            notes = st.text_area("Observações")
            if st.form_submit_button("Registrar movimentação"):
                if not description.strip():
                    st.error("Informe a descrição da movimentação.")
                else:
                    try:
                        add_cash_movement(session_id, movement_type, description, amount, payment_method, notes)
                        st.success("Movimentação registrada.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

        movimentos = query_df(
            """
            SELECT cm.id AS ID, cm.created_at AS Data, cm.movement_type AS Tipo, cm.description AS Descrição,
                   cm.amount AS Valor, cm.payment_method AS Forma_pagamento,
                   CASE WHEN c.number IS NULL THEN '-' ELSE '#' || c.number END AS Comanda,
                   COALESCE(u.name, '-') AS Usuário, cm.notes AS Observações
            FROM cash_movements cm
            LEFT JOIN commands c ON c.id = cm.command_id
            LEFT JOIN system_users u ON u.id = cm.created_by
            WHERE cm.session_id = ?
            ORDER BY cm.id DESC
            """,
            (session_id,),
        )
        st.write("**Movimentações do caixa atual**")
        pretty_dataframe(movimentos, width="stretch", hide_index=True)

        st.divider()
        st.write("**Fechar caixa**")
        with st.form("form_close_cash"):
            closing_amount = st.number_input("Valor conferido no fechamento", min_value=0.0, value=float(expected), step=10.0, format="%.2f")
            close_notes = st.text_area("Observações do fechamento")
            diff_preview = float(closing_amount) - float(expected)
            st.metric("Diferença prevista", brl(diff_preview))
            confirm = st.checkbox("Confirmo que conferi o caixa e desejo fechar")
            if st.form_submit_button("Fechar caixa", type="primary"):
                if not confirm:
                    st.warning("Marque a confirmação antes de fechar o caixa.")
                else:
                    try:
                        expected_final, difference = close_cash(session_id, closing_amount, close_notes)
                        st.success(f"Caixa fechado. Esperado: {brl(expected_final)} | Diferença: {brl(difference)}")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

    with tab2:
        sessions = query_df(
            """
            SELECT cs.id AS ID, cs.status AS Status, cs.opened_at AS Abertura, cs.closed_at AS Fechamento,
                   cs.opening_amount AS Valor_inicial, cs.expected_amount AS Valor_esperado,
                   cs.closing_amount AS Valor_conferido, cs.difference_amount AS Diferença,
                   COALESCE(u1.name, '-') AS Aberto_por, COALESCE(u2.name, '-') AS Fechado_por,
                   cs.notes AS Observações
            FROM cash_sessions cs
            LEFT JOIN system_users u1 ON u1.id = cs.opened_by
            LEFT JOIN system_users u2 ON u2.id = cs.closed_by
            ORDER BY cs.id DESC
            """
        )
        pretty_dataframe(sessions, width="stretch", hide_index=True)
        csv_download(sessions, "historico_caixas.csv", "Baixar histórico de caixas em CSV")

def page_dashboard():
    def kpi_card_html(icon, title, value, subtitle=""):
        return f"""
            <div class="dash-kpi">
                <div class="dash-kpi-top">
                    <div>
                        <div class="dash-kpi-title">{title}</div>
                        <div class="dash-kpi-value">{value}</div>
                    </div>
                    <div class="dash-kpi-icon">{icon}</div>
                </div>
                <div class="dash-kpi-sub">{subtitle}</div>
            </div>
        """

    st.markdown(
        """
        <div class="dashboard-hero">
            <div>
                <h2>Dashboard operacional</h2>
                <p>Visão geral do caixa, jogos, comandas, estoque e resultado financeiro no novo visual Tactical Couple.</p>
            </div>
            <div class="dashboard-status-pill">🎯 Tactical Couple</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    open_cash_session = get_open_cash_session()
    latest_open_event = query_df("SELECT id, event_date, name, status FROM events WHERE status = 'Aberto' ORDER BY event_date DESC, id DESC LIMIT 1")

    status_cols = st.columns([1.1, 1.4, 1.1])
    with status_cols[0]:
        if open_cash_session is not None:
            st.success(f"Caixa aberto • Saldo esperado: {brl(cash_expected_amount(int(open_cash_session['id'])))}")
        else:
            st.warning("Caixa fechado. Abra o caixa antes de iniciar a operação.")
    with status_cols[1]:
        if not latest_open_event.empty:
            ev = latest_open_event.iloc[0]
            st.info(f"Jogo atual: {format_date_br(ev['event_date'])} — {ev['name']}")
        else:
            st.info("Nenhum jogo aberto no momento.")
    with status_cols[2]:
        st.caption("Filtre o período para atualizar os indicadores abaixo.")

    with st.container():
        st.markdown('<div class="dash-card-title">📅 Período do dashboard</div>', unsafe_allow_html=True)
        start_date, end_date = render_period_filter()

    values = kpis(start_date, end_date)

    open_cmd_count_df = query_df("SELECT COUNT(*) AS total FROM commands WHERE status = 'Aberta'")
    open_cmd_count = int(open_cmd_count_df.iloc[0]['total']) if not open_cmd_count_df.empty else 0
    low_stock_count_df = query_df("SELECT COUNT(*) AS total FROM products WHERE active = 1 AND stock_qty <= min_stock")
    low_stock_count = int(low_stock_count_df.iloc[0]['total']) if not low_stock_count_df.empty else 0
    caixa_esperado = cash_expected_amount(int(open_cash_session['id'])) if open_cash_session is not None else 0

    st.markdown('<div class="dash-section-title">Indicadores principais</div>', unsafe_allow_html=True)
    dashboard_cards = [
        ("💰", "Receita total", brl(values["receita_total"]), "Entradas + comandas + consumo"),
        ("📈", "Lucro líquido", brl(values["lucro_liquido"]), f"Margem líquida: {values['margem_liquida']:.1f}%"),
        ("🧮", "Custos totais", brl(values["custos_totais"]), "Custo vendido + despesas + compras"),
        ("💵", "Caixa esperado", brl(caixa_esperado), "Saldo previsto do caixa aberto"),
        ("🧾", "Comandas abertas", f"{open_cmd_count}", "Comandas ainda não fechadas"),
        ("👥", "Jogadores", f"{int(values['jogadores'])}", "Total de jogadores no período"),
        ("🎟️", "Ticket médio", brl(values["ticket_medio"]), "Receita total / jogadores"),
        ("📦", "Estoque baixo", f"{low_stock_count}", "Produtos abaixo do mínimo"),
        ("🛒", "Compras de estoque", brl(values["compras_estoque"]), "Entradas registradas no estoque"),
        ("🏷️", "Custo vendido", brl(values["custo_produtos"]), "Custo dos produtos vendidos"),
        ("💸", "Despesas operacionais", brl(values["despesas_operacionais"]), "Despesas sem compra de estoque"),
        ("📦", "Valor em estoque", brl(values["valor_em_estoque"]), "Custo estimado parado em estoque"),
    ]
    for i in range(0, len(dashboard_cards), 3):
        cols = st.columns(3)
        for col, card in zip(cols, dashboard_cards[i:i + 3]):
            icon, title, value, subtitle = card
            with col:
                st.markdown(kpi_card_html(icon, title, value, subtitle), unsafe_allow_html=True)

    st.markdown('<div class="dash-section-title">Operação em tempo real</div>', unsafe_allow_html=True)
    left_col, right_col = st.columns([1.2, 1])

    with left_col:
        st.markdown('<div class="dash-card-title">🧾 Comandas abertas</div>', unsafe_allow_html=True)
        open_cmds = query_df(
            """
            SELECT c.number AS Comanda,
                   COALESCE(o.name, '-') AS Operador_jogador,
                   COALESCE(e.name, '-') AS Jogo,
                   COALESCE(c.entry_value, 0) + COALESCE(SUM(s.revenue), 0) - COALESCE(c.discount_amount, 0) AS Total
            FROM commands c
            LEFT JOIN operators o ON o.id = c.operator_id
            LEFT JOIN events e ON e.id = c.event_id
            LEFT JOIN sales s ON s.command_id = c.id
            WHERE c.status = 'Aberta'
            GROUP BY c.id, o.name, e.name
            ORDER BY c.opened_at DESC, c.number DESC
            LIMIT 10
            """
        )
        if open_cmds.empty:
            st.info("Nenhuma comanda aberta.")
        else:
            open_cmds["Total"] = open_cmds["Total"].map(brl)
            pretty_dataframe(open_cmds, width="stretch", hide_index=True)

        st.markdown('<div class="dash-card-title">📊 Resultado por categoria</div>', unsafe_allow_html=True)
        vendas_cat = query_df(
            """
            SELECT p.category AS Categoria,
                   SUM(s.qty) AS Quantidade,
                   SUM(s.revenue) AS Receita,
                   SUM(s.cogs) AS Custo,
                   SUM(s.revenue - s.cogs) AS Lucro_bruto
            FROM sales s
            JOIN products p ON p.id = s.product_id
            WHERE DATE(s.sale_date) BETWEEN DATE(?) AND DATE(?)
            GROUP BY p.category
            ORDER BY SUM(s.revenue) DESC
            """,
            (str(start_date), str(end_date)),
        )
        if vendas_cat.empty:
            st.info("Nenhuma venda/consumo no período.")
        else:
            chart_cat = vendas_cat.copy()
            st.bar_chart(chart_cat.set_index("Categoria")[["Receita", "Custo", "Lucro_bruto"]])
            for col in ["Receita", "Custo", "Lucro_bruto"]:
                vendas_cat[col] = vendas_cat[col].map(brl)
            pretty_dataframe(vendas_cat, width="stretch", hide_index=True)

    with right_col:
        st.markdown('<div class="dash-card-title">🚨 Alertas de estoque</div>', unsafe_allow_html=True)
        low_stock_dash = query_df(
            """
            SELECT sku AS SKU, name AS Produto, category AS Categoria,
                   stock_qty AS Estoque, min_stock AS Mínimo
            FROM products
            WHERE active = 1 AND stock_qty <= min_stock
            ORDER BY (stock_qty - min_stock), name
            LIMIT 10
            """
        )
        if low_stock_dash.empty:
            st.success("Nenhum produto abaixo do mínimo.")
        else:
            pretty_dataframe(low_stock_dash, width="stretch", hide_index=True)

        st.markdown('<div class="dash-card-title">🏅 Top operadores / jogadores</div>', unsafe_allow_html=True)
        top_ops = query_df(
            """
            WITH sales_by_cmd AS (
                SELECT command_id, SUM(revenue) AS consumo
                FROM sales
                GROUP BY command_id
            )
            SELECT COALESCE(o.name, '-') AS Operador_jogador,
                   COUNT(c.id) AS Comandas,
                   SUM(COALESCE(c.entry_value, 0) + COALESCE(sb.consumo, 0) - COALESCE(c.discount_amount, 0)) AS Total_gasto
            FROM commands c
            LEFT JOIN operators o ON o.id = c.operator_id
            LEFT JOIN sales_by_cmd sb ON sb.command_id = c.id
            WHERE c.status <> 'Cancelada'
              AND DATE(c.opened_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY o.id, o.name
            ORDER BY SUM(COALESCE(c.entry_value, 0) + COALESCE(sb.consumo, 0) - COALESCE(c.discount_amount, 0)) DESC
            LIMIT 10
            """,
            (str(start_date), str(end_date)),
        )
        if top_ops.empty:
            st.info("Nenhum operador/jogador no período.")
        else:
            top_ops["Total_gasto"] = top_ops["Total_gasto"].fillna(0).map(brl)
            pretty_dataframe(top_ops, width="stretch", hide_index=True)

    st.markdown('<div class="dash-section-title">Financeiro e jogos</div>', unsafe_allow_html=True)
    fin_col, event_col = st.columns([1, 1.35])

    with fin_col:
        st.markdown('<div class="dash-card-title">📋 DRE simplificada</div>', unsafe_allow_html=True)
        dre = pd.DataFrame(
            [
                ("Receita com entradas", values["receita_entradas"]),
                ("Receita com vendas/consumo", values["receita_vendas"]),
                ("(-) Descontos em comandas", -values["descontos"]),
                ("Receita total", values["receita_total"]),
                ("(-) Custo dos produtos vendidos", -values["custo_produtos"]),
                ("Lucro bruto", values["lucro_bruto"]),
                ("(-) Despesas operacionais", -values["despesas_operacionais"]),
                ("Lucro operacional", values["lucro_liquido"]),
                ("Compras de estoque no período", values["compras_estoque"]),
                ("Valor estimado em estoque", values["valor_em_estoque"]),
            ],
            columns=["Indicador", "Valor"],
        )
        pretty_dataframe(dre.assign(Valor=dre["Valor"].map(brl)), width="stretch", hide_index=True)

    with event_col:
        st.markdown('<div class="dash-card-title">🎯 Resultado por jogo</div>', unsafe_allow_html=True)
        eventos_resultado = query_df(
            """
            SELECT e.event_date AS Data,
                   e.name AS Jogo,
                   e.players AS Total_jogadores,
                   e.entry_revenue AS Receita_entradas,
                   COALESCE(SUM(s.revenue), 0) AS Receita_consumo,
                   COALESCE((SELECT SUM(discount_amount) FROM commands c WHERE c.event_id = e.id AND c.status = 'Fechada'), 0) AS Descontos,
                   COALESCE(SUM(s.cogs), 0) AS Custo_produtos,
                   COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id), 0) AS Despesas,
                   e.entry_revenue + COALESCE(SUM(s.revenue), 0) - COALESCE((SELECT SUM(discount_amount) FROM commands c WHERE c.event_id = e.id AND c.status = 'Fechada'), 0) - COALESCE(SUM(s.cogs), 0) - COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id), 0) AS Lucro_liquido
            FROM events e
            LEFT JOIN sales s ON s.event_id = e.id
            WHERE DATE(e.event_date) BETWEEN DATE(?) AND DATE(?)
            GROUP BY e.id
            ORDER BY e.event_date DESC, e.id DESC
            """,
            (str(start_date), str(end_date)),
        )
        if eventos_resultado.empty:
            st.info("Nenhum jogo no período.")
        else:
            chart_df = eventos_resultado.copy()
            chart_df["Jogo_label"] = chart_df["Data"].apply(format_date_br) + " — " + chart_df["Jogo"].astype(str)
            st.bar_chart(chart_df.set_index("Jogo_label")[["Receita_entradas", "Receita_consumo", "Lucro_liquido"]])
            money_cols = ["Receita_entradas", "Receita_consumo", "Descontos", "Custo_produtos", "Despesas", "Lucro_liquido"]
            table_events = format_date_columns(eventos_resultado.copy())
            for col in money_cols:
                if col in table_events.columns:
                    table_events[col] = table_events[col].map(brl)
            pretty_dataframe(table_events, width="stretch", hide_index=True)

    with st.expander("Ver comandas do período"):
        comandas = query_df(
            """
            SELECT c.number AS Comanda,
                   c.status AS Status,
                   c.opened_at AS Data,
                   COALESCE(e.name, '-') AS Jogo,
                   COALESCE(o.name, '-') AS Operador_jogador,
                   c.entry_type AS Tipo_entrada,
                   c.entry_value AS Valor_entrada,
                   COALESCE(SUM(s.revenue), 0) AS Consumo,
                   COALESCE(c.discount_percent, 0) AS Desconto_percentual,
                   COALESCE(c.discount_amount, 0) AS Desconto_valor,
                   c.entry_value + COALESCE(SUM(s.revenue), 0) - COALESCE(c.discount_amount, 0) AS Total
            FROM commands c
            LEFT JOIN events e ON e.id = c.event_id
            LEFT JOIN operators o ON o.id = c.operator_id
            LEFT JOIN sales s ON s.command_id = c.id
            WHERE DATE(c.opened_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY c.id, o.name, e.name
            ORDER BY c.opened_at DESC, c.number DESC
            """,
            (str(start_date), str(end_date)),
        )
        if comandas.empty:
            st.info("Nenhuma comanda no período.")
        else:
            money_cols = ["Valor_entrada", "Consumo", "Desconto_valor", "Total"]
            comandas_fmt = format_date_columns(comandas.copy())
            for col in money_cols:
                if col in comandas_fmt.columns:
                    comandas_fmt[col] = comandas_fmt[col].map(brl)
            pretty_dataframe(comandas_fmt, width="stretch", hide_index=True)


def page_operators():
    st.subheader("Operadores")
    st.caption("Cadastre os operadores/jogadores do esporte. Neste sistema, operador é o jogador/cliente que usa a comanda. Nome é obrigatório; onde mora, equipe e telefone são opcionais.")

    pending_prompt = st.session_state.get("operator_created_prompt")
    if pending_prompt:
        open_cash_session = get_open_cash_session()
        has_open_cash = open_cash_session is not None

        latest_event_df = query_df("SELECT id, event_date, name, status FROM events ORDER BY event_date DESC, id DESC LIMIT 1")
        has_latest_event = not latest_event_df.empty
        latest_event_open = False
        latest_event_label = ""

        if has_latest_event:
            latest_event = latest_event_df.iloc[0]
            latest_event_status = str(latest_event.get("status", "")).strip().lower()
            latest_event_open = latest_event_status == "aberto"
            latest_event_label = f"{format_date_br(latest_event['event_date'])} — {latest_event['name']}"

        # O popup só pode aparecer quando AS 3 condições forem verdadeiras:
        # 1) existe caixa aberto; 2) existe jogo cadastrado; 3) o jogo mais recente está Aberto.
        can_create_command_now = has_open_cash and has_latest_event and latest_event_open

        if can_create_command_now:
            @st.dialog("Operador/jogador cadastrado", width="large")
            def operator_created_dialog():
                st.success(f"{pending_prompt.get('name', 'Operador/jogador')} foi cadastrado com sucesso.")
                st.write("Você deseja criar uma comanda para esse operador?")
                st.caption(f"Caixa aberto e jogo mais recente disponível: {latest_event_label}")
                c_yes, c_no = st.columns(2)
                if c_yes.button("Sim, criar comanda", type="primary", width="stretch"):
                    if pending_prompt.get("label"):
                        st.session_state["cmd_operator_after_create"] = pending_prompt["label"]
                    st.session_state.pop("operator_created_prompt", None)
                    st.session_state["force_sidebar_page"] = "Comandas"
                    st.rerun()
                if c_no.button("Não", width="stretch"):
                    st.session_state.pop("operator_created_prompt", None)
                    st.rerun()
            operator_created_dialog()
        else:
            st.success(f"{pending_prompt.get('name', 'Operador/jogador')} foi cadastrado com sucesso.")
            if not has_open_cash:
                st.warning("O popup para criar comanda não foi exibido porque não há caixa aberto. Abra o caixa antes de criar comandas.")
            if not has_latest_event:
                st.warning("O popup para criar comanda não foi exibido porque ainda não há jogo cadastrado.")
            elif not latest_event_open:
                st.warning(f"O popup para criar comanda não foi exibido porque o jogo mais recente não está aberto: {latest_event_label}.")
            st.session_state.pop("operator_created_prompt", None)

    operator_nonce = st.session_state.get("operator_create_form_nonce", 0)
    with st.form(f"form_operator_create_v29_{operator_nonce}", clear_on_submit=True):
        c1, c2 = st.columns(2)
        name = c1.text_input("Nome do operador/jogador *", key=f"operator_name_create_v29_{operator_nonce}")
        residence = c2.text_input("Onde mora / bairro / cidade", key=f"operator_residence_create_v29_{operator_nonce}")
        c3, c4 = st.columns(2)
        team = c3.text_input("Equipe", key=f"operator_team_create_v29_{operator_nonce}")
        phone = c4.text_input("Telefone", key=f"operator_phone_create_v29_{operator_nonce}", max_chars=11, placeholder="Somente números, até 11 dígitos")
        submitted = st.form_submit_button("Cadastrar operador/jogador")
        if submitted:
            if not name.strip():
                st.error("Informe o nome do operador/jogador.")
            else:
                phone_clean = only_digits_11(phone)
                new_operator_id = execute("INSERT INTO operators (name, residence, team, phone, cpf, active) VALUES (?, ?, ?, ?, NULL, 1)", (name.strip(), residence.strip() or None, team.strip() or None, phone_clean or None))
                label_parts = [name.strip()]
                if team.strip():
                    label_parts.append("Equipe " + team.strip())
                if phone_clean:
                    label_parts.append("Tel " + phone_clean)
                if residence.strip():
                    label_parts.append(residence.strip())
                st.session_state["operator_created_prompt"] = {
                    "id": int(new_operator_id),
                    "name": name.strip(),
                    "label": " — ".join(label_parts),
                }
                st.session_state["operator_create_form_nonce"] = operator_nonce + 1
                st.rerun()

    st.divider()
    st.write("**Operadores/jogadores cadastrados**")
    f1, f2 = st.columns([1, 2])
    filtro = f1.radio("Mostrar", ["Ativos", "Inativos", "Todos"], horizontal=True)
    busca = f2.text_input("Pesquisar operador/jogador", placeholder="Busque por nome, equipe, telefone ou onde mora")

    conditions = []
    params = []
    if filtro == "Ativos":
        conditions.append("active = 1")
    elif filtro == "Inativos":
        conditions.append("active = 0")

    if busca.strip():
        like = f"%{busca.strip().lower()}%"
        conditions.append("(LOWER(COALESCE(name, '')) LIKE ? OR LOWER(COALESCE(residence, '')) LIKE ? OR LOWER(COALESCE(team, '')) LIKE ? OR LOWER(COALESCE(phone, '')) LIKE ?)")
        params.extend([like, like, like, like])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    df = query_df(
        f"""
        SELECT
            o.id AS ID,
            o.name AS Nome,
            o.residence AS Onde_mora,
            o.team AS Equipe,
            o.phone AS Telefone,
            o.created_at AS Data_cadastro,
            (
                SELECT COALESCE(strftime('%d/%m/%Y', e.event_date), '') || ' — ' || COALESCE(e.name, '-')
                FROM commands c
                LEFT JOIN events e ON e.id = c.event_id
                WHERE c.operator_id = o.id
                ORDER BY COALESCE(c.opened_at, c.created_at::text) DESC, c.id DESC
                LIMIT 1
            ) AS Ultimo_jogo,
            CASE WHEN o.active = 1 THEN 'Ativo' ELSE 'Inativo' END AS Status
        FROM operators o
        {where}
        ORDER BY o.name
        """,
        tuple(params),
    )
    if df.empty:
        st.info("Nenhum operador/jogador encontrado para os filtros informados.")
        return
    df["Data_cadastro"] = df["Data_cadastro"].apply(format_date_br)
    df["Ultimo_jogo"] = df["Ultimo_jogo"].fillna("-").replace("", "-")
    view = df.copy()
    view["Selecionar"] = False
    view = view[["Selecionar", "ID", "Nome", "Onde_mora", "Equipe", "Telefone", "Data_cadastro", "Ultimo_jogo", "Status"]]
    edited = st.data_editor(
        view,
        width="stretch",
        hide_index=True,
        disabled=["ID", "Nome", "Onde_mora", "Equipe", "Telefone", "Data_cadastro", "Ultimo_jogo", "Status"],
        column_config=default_column_config(view, {"Selecionar": st.column_config.CheckboxColumn("Selecionar")}),
        key="operators_editor",
    )
    selected = edited[edited["Selecionar"] == True] if "Selecionar" in edited.columns else pd.DataFrame()
    c1, c2, c3 = st.columns(3)
    if c1.button("Inativar selecionado"):
        if selected.empty or len(selected) != 1:
            st.warning("Selecione exatamente um operador/jogador.")
        else:
            execute("UPDATE operators SET active = 0 WHERE id = ?", (int(selected.iloc[0]["ID"]),))
            st.success("Operador/jogador inativado.")
            st.rerun()
    if c2.button("Restaurar selecionado"):
        if selected.empty or len(selected) != 1:
            st.warning("Selecione exatamente um operador/jogador.")
        else:
            execute("UPDATE operators SET active = 1 WHERE id = ?", (int(selected.iloc[0]["ID"]),))
            st.success("Operador/jogador restaurado.")
            st.rerun()
    if c3.button("Alterar selecionado"):
        if selected.empty or len(selected) != 1:
            st.warning("Selecione exatamente um operador/jogador para alterar.")
        else:
            st.session_state["operator_edit_id"] = int(selected.iloc[0]["ID"])

    edit_id = st.session_state.get("operator_edit_id")
    if edit_id:
        atual = query_df("SELECT id, name, residence, team, phone FROM operators WHERE id = ?", (edit_id,))
        if not atual.empty:
            st.write("**Alterar operador/jogador selecionado**")
            with st.form("form_operator_update"):
                c1, c2 = st.columns(2)
                new_name = c1.text_input("Nome", value=str(atual.iloc[0]["name"]))
                new_residence = c2.text_input("Onde mora / bairro / cidade", value=str(atual.iloc[0]["residence"] or ""))
                c3, c4 = st.columns(2)
                new_team = c3.text_input("Equipe", value=str(atual.iloc[0]["team"] or ""))
                new_phone = c4.text_input("Telefone", value=only_digits_11(atual.iloc[0]["phone"]), max_chars=11, placeholder="Somente números, até 11 dígitos")
                if st.form_submit_button("Salvar alteração"):
                    if not new_name.strip():
                        st.error("Informe o nome do operador/jogador.")
                    else:
                        new_phone_clean = only_digits_11(new_phone)
                        execute("UPDATE operators SET name = ?, residence = ?, team = ?, phone = ?, cpf = NULL WHERE id = ?", (new_name.strip(), new_residence.strip() or None, new_team.strip() or None, new_phone_clean or None, edit_id))
                        st.session_state.pop("operator_edit_id", None)
                        st.success("Operador/jogador alterado.")
                        st.rerun()

def page_commands():
    st.subheader("Comandas")
    st.caption("A comanda sempre precisa estar vinculada a um jogo e a um operador/jogador cadastrado previamente.")

    tab_create, tab_operate, tab_history = st.tabs(["1. Abrir comanda", "2. Operar comanda", "3. Histórico"])

    with tab_create:
        next_num = next_command_number()
        st.metric("Próxima comanda", f"#{next_num}")

        events = event_options()
        ops = operators_options()

        if events.empty:
            st.warning("Cadastre um jogo na aba Jogos antes de abrir comandas.")
            return

        event_labels = [f"{format_date_br(r['event_date'])} — {r['name']}" for _, r in events.iterrows()]
        event_ids = {f"{format_date_br(r['event_date'])} — {r['name']}": int(r["id"]) for _, r in events.iterrows()}

        def op_label(r):
            parts = [str(r['name'])]
            if pd.notna(r.get('team')) and str(r.get('team')).strip():
                parts.append('Equipe ' + str(r['team']))
            if pd.notna(r.get('phone')) and str(r.get('phone')).strip():
                parts.append('Tel ' + str(r['phone']))
            if pd.notna(r.get('residence')) and str(r.get('residence')).strip():
                parts.append(str(r['residence']))
            return ' — '.join(parts)

        SELECT_OPERATOR_OPTION = "Selecione o operador/jogador"
        NEW_OPERATOR_OPTION = "➕ Cadastrar novo operador/jogador"
        op_labels_existing = [op_label(r) for _, r in ops.iterrows()]
        op_labels = [SELECT_OPERATOR_OPTION, NEW_OPERATOR_OPTION] + op_labels_existing
        op_ids = {op_label(r): int(r["id"]) for _, r in ops.iterrows()}

        cmd_nonce = st.session_state.get("command_create_form_nonce", 0)
        operator_key = f"cmd_operator_v58_{cmd_nonce}"

        @st.dialog("Cadastrar novo operador/jogador", width="large")
        def quick_operator_dialog():
            st.caption("Cadastre o operador/jogador sem sair da abertura da comanda.")
            with st.form("form_quick_operator_from_command", clear_on_submit=True):
                q1, q2 = st.columns(2)
                quick_name = q1.text_input("Nome do operador/jogador *")
                quick_residence = q2.text_input("Onde mora / bairro / cidade")
                q3, q4 = st.columns(2)
                quick_team = q3.text_input("Equipe")
                quick_phone = q4.text_input("Telefone", max_chars=11, placeholder="Somente números, até 11 dígitos")
                qa, qb = st.columns(2)
                if qa.form_submit_button("Cadastrar operador", type="primary"):
                    if not quick_name.strip():
                        st.error("O nome é obrigatório.")
                    else:
                        quick_phone_clean = only_digits_11(quick_phone)
                        execute(
                            "INSERT INTO operators (name, residence, team, phone, cpf, active) VALUES (?, ?, ?, ?, NULL, 1)",
                            (
                                quick_name.strip(),
                                quick_residence.strip() or None,
                                quick_team.strip() or None,
                                quick_phone_clean or None,
                            ),
                        )
                        novo_label_parts = [quick_name.strip()]
                        if quick_team.strip():
                            novo_label_parts.append("Equipe " + quick_team.strip())
                        if quick_phone_clean:
                            novo_label_parts.append("Tel " + quick_phone_clean)
                        if quick_residence.strip():
                            novo_label_parts.append(quick_residence.strip())
                        st.session_state["cmd_operator_after_create"] = " — ".join(novo_label_parts)
                        st.success("Operador/jogador cadastrado com sucesso.")
                        st.rerun()
                if qb.form_submit_button("Cancelar"):
                    st.session_state[operator_key] = SELECT_OPERATOR_OPTION
                    st.rerun()

        st.write("**Vincular comanda ao jogo e operador/jogador**")
        event_label = st.selectbox("Jogo vinculado *", event_labels, key="cmd_create_event_v18")

        after_create_label = st.session_state.pop("cmd_operator_after_create", None)
        if after_create_label and after_create_label in op_labels:
            st.session_state[operator_key] = after_create_label
        elif operator_key not in st.session_state:
            st.session_state[operator_key] = SELECT_OPERATOR_OPTION

        render_field_tooltip("Operador/jogador da comanda *", "Selecione um operador/jogador cadastrado ou use a opção de cadastro rápido sem sair da abertura da comanda.")
        operator_label = st.selectbox(
            "Operador/jogador da comanda *",
            op_labels,
            index=0,
            key=operator_key,
            label_visibility="collapsed",
        )

        if operator_label == NEW_OPERATOR_OPTION:
            quick_operator_dialog()
            st.info("Cadastre o operador/jogador no popup para continuar a abertura da comanda, ou clique em Cancelar para voltar à seleção.")
        elif operator_label == SELECT_OPERATOR_OPTION or operator_label not in op_ids:
            st.warning("Selecione um operador/jogador cadastrado ou use a opção de cadastro rápido para abrir a comanda.")
        else:
            selected_operator_id = op_ids[operator_label]
            existing = existing_command_for_operator(event_ids[event_label], selected_operator_id)

            if existing:
                st.error(
                    f"Este operador/jogador já possui a comanda #{int(existing['number'])} neste jogo. "
                    "Selecione essa comanda na aba Operar comanda ou cancele a comanda existente antes de abrir outra."
                )
            else:
                st.write("**Tipo de entrada e valores**")
                entry_type = st.selectbox("Tipo de entrada", ["Equipamento próprio", "Aluguel", "Sem entrada"], key="cmd_create_entry_type_v18")

                # O valor da entrada é puxado automaticamente dos valores padrão cadastrados no jogo.
                # Estes campos ficam fora do formulário para o Streamlit atualizar o valor assim que o tipo de entrada mudar.
                selected_event = query_df("SELECT rental_unit_price, own_equipment_unit_price FROM events WHERE id = ?", (event_ids[event_label],))
                default_entry_value = 0.0
                if not selected_event.empty:
                    if entry_type == "Aluguel":
                        default_entry_value = float(selected_event.iloc[0]["rental_unit_price"] or 0)
                    elif entry_type == "Equipamento próprio":
                        default_entry_value = float(selected_event.iloc[0]["own_equipment_unit_price"] or 0)

                if entry_type == "Aluguel":
                    st.info(f"Valor puxado do jogo para entrada com aluguel: {brl(default_entry_value)}")
                elif entry_type == "Equipamento próprio":
                    st.info(f"Valor puxado do jogo para entrada com equipamento próprio: {brl(default_entry_value)}")
                else:
                    st.info("Tipo sem entrada selecionado: valor da entrada será R$ 0,00.")

                # Força o campo de valor a ser recriado quando mudar o jogo ou o tipo de entrada,
                # garantindo que ele puxe corretamente o valor padrão cadastrado no jogo selecionado.
                entry_key_suffix = f"{event_ids[event_label]}_{entry_type.replace(' ', '_').replace('ó', 'o').replace('á', 'a')}"
                with st.form(f"form_command_create_v29_{cmd_nonce}", clear_on_submit=True):
                    opened_at = st.date_input("Data de abertura", value=date.today(), format="DD/MM/YYYY", key=f"cmd_opened_at_v29_{cmd_nonce}")
                    customer_name = st.text_input("Nome complementar/apelido (opcional)", key=f"cmd_customer_v29_{cmd_nonce}")
                    render_field_tooltip("Valor da entrada", "Esse valor vem automaticamente dos valores padrão cadastrados na abertura do jogo, mas pode ser ajustado nesta comanda se necessário. Caso a entrada seja cortesia, isso será aplicado no fechamento da comanda.")
                    entry_value = st.number_input(
                        "Valor da entrada",
                        min_value=0.0,
                        value=float(default_entry_value),
                        step=10.0,
                        format="%.2f",
                        key=f"cmd_entry_value_v38_{cmd_nonce}_{entry_key_suffix}",
                        label_visibility="collapsed",
                    )
                    notes = st.text_area("Observações", key=f"cmd_notes_v29_{cmd_nonce}")
                    if st.form_submit_button("Abrir comanda"):
                        try:
                            nome_operador = operator_label.split(' — ')[0]
                            charged_entry_value = float(entry_value or 0)
                            command_id = create_command(
                                opened_at,
                                event_ids[event_label],
                                selected_operator_id,
                                customer_name.strip() or nome_operador,
                                entry_type,
                                charged_entry_value,
                                notes,
                                entry_original_value=float(entry_value or 0),
                                entry_courtesy=False,
                                entry_courtesy_reason="",
                            )
                            numero = query_df("SELECT number FROM commands WHERE id = ?", (command_id,))["number"].iloc[0]
                            st.session_state["command_create_form_nonce"] = cmd_nonce + 1
                            st.success(f"Comanda #{int(numero)} aberta e vinculada ao jogo, ao operador/jogador e ao tipo de entrada selecionado.")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))

    with tab_operate:
        abertas = command_options("Aberta")
        if abertas.empty:
            st.info("Nenhuma comanda aberta.")
        else:
            command_map = {f"#{int(r['number'])} — {r['customer_name'] or 'sem nome'} — {r['event_name']} — {brl(float(r['entry_value']) + float(r['total']) - float(r['entry_value']))}": int(r["id"]) for _, r in abertas.iterrows()}
            label = st.selectbox("Comanda aberta", list(command_map.keys()))
            command_id = command_map[label]
            cmd = get_command(command_id)

            vendas_comanda = query_df(
                """
                SELECT s.id AS ID, s.sale_date AS Data, p.sku AS SKU, p.category AS Categoria, p.name AS Produto,
                       s.qty AS Quantidade, s.unit_price AS Preço_unitário, s.revenue AS Total, s.cogs AS Custo, s.revenue - s.cogs AS Lucro,
                       COALESCE(s.notes, '') AS Observação
                FROM sales s
                JOIN products p ON p.id = s.product_id
                WHERE s.command_id = ?
                ORDER BY s.id DESC
                """,
                (command_id,),
            )
            consumo = float(vendas_comanda["Total"].sum()) if not vendas_comanda.empty else 0
            custo = float(vendas_comanda["Custo"].sum()) if not vendas_comanda.empty else 0
            entrada = float(cmd["entry_value"] or 0)
            entrada_original = float(cmd.get("entry_original_value", entrada) or entrada or 0)
            entrada_cortesia = bool(int(cmd.get("entry_courtesy", 0) or 0))
            entrada_cortesia_motivo = str(cmd.get("entry_courtesy_reason", "") or "")
            desconto_atual = float(cmd.get("discount_amount", 0) or 0)
            subtotal = entrada + consumo
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Comanda", f"#{int(cmd['number'])}")
            c2.metric("Entrada", brl(entrada))
            c3.metric("Consumo", brl(consumo))
            c4.metric("Total", brl(subtotal - desconto_atual))
            st.caption(f"Jogo: {cmd['event_name']} | Operador/jogador: {cmd['operator_name']} | Jogador/cliente: {cmd['customer_name'] or '-'}")

            st.write("**Adicionar produto na comanda**")
            products = product_options()
            ops = operators_options()
            if products.empty:
                st.info("Cadastre produtos para vender.")
            elif ops.empty:
                st.warning("Cadastre um operador/jogador ativo antes de lançar consumo.")
            else:
                product_map = {f"{(str(r['sku']) + ' — ') if pd.notna(r['sku']) and str(r['sku']).strip() else ''}{r['name']} — {r['category']} — estoque: {r['stock_qty']:g} — preço: {brl(r['sale_price'])}": int(r["id"]) for _, r in products.iterrows()}
                op_map = {str(r['name']): int(r['id']) for _, r in ops.iterrows()}

                # O produto fica fora do formulário para o Streamlit atualizar a tela assim que o item for alterado.
                # A primeira opção é vazia para evitar pré-seleção automática de produto na comanda.
                product_placeholder = "Selecione um produto"
                product_labels = [product_placeholder] + list(product_map.keys())
                produto_label = st.selectbox(
                    "Produto",
                    product_labels,
                    index=0,
                    key=f"cmd_sale_product_select_{command_id}",
                )

                produto_id = product_map.get(produto_label)
                preco_padrao = 0.0
                if produto_id:
                    selected = products[products["id"] == produto_id].iloc[0]
                    preco_padrao = float(selected["sale_price"] or 0)
                    st.info(f"Preço de venda cadastrado para este item: {brl(preco_padrao)}")
                else:
                    st.info("Selecione um produto para puxar automaticamente o preço de venda cadastrado.")

                with st.form("form_command_sale_v19"):
                    cc1, cc2 = st.columns(2)
                    qty = cc1.number_input("Quantidade", min_value=0.01, value=1.0, step=1.0)
                    with cc2:
                        render_field_tooltip("Preço unitário", "Valor puxado automaticamente do preço de venda cadastrado no produto. Pode ser editado para exceções.")
                        unit_price = st.number_input(
                            "Preço unitário",
                            min_value=0.0,
                            value=preco_padrao,
                            step=0.10,
                            format="%.2f",
                            key=f"cmd_sale_unit_price_{command_id}_{produto_id or 'none'}",
                            label_visibility="collapsed",
                        )
                    st.info(f"Este consumo será vinculado ao operador/jogador da comanda: {cmd['operator_name']}")
                    notes = st.text_input("Observação")
                    if st.form_submit_button("Adicionar à comanda"):
                        if not produto_id:
                            st.warning("Selecione um produto antes de adicionar à comanda.")
                        else:
                            try:
                                add_sale(date.today(), produto_id, int(cmd["event_id"]), qty, unit_price, notes or f"Comanda #{int(cmd['number'])}", command_id=command_id, operator_id=int(cmd["operator_id"]) if pd.notna(cmd["operator_id"]) else None)
                                st.success("Produto adicionado à comanda, estoque baixado e jogo atualizado.")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))

            st.write("**Itens da comanda**")
            if vendas_comanda.empty:
                st.info("Esta comanda ainda não possui itens de consumo.")
            else:
                st.caption("Os itens agora aparecem em cards. Use Editar para alterar produto, quantidade, preço ou observação; use Excluir para remover um item.")

                def _sale_item_details(sale_id):
                    return query_df(
                        """
                        SELECT s.id AS ID, s.sale_date AS Data, s.product_id AS product_id,
                               p.sku AS SKU, p.category AS Categoria, p.name AS Produto,
                               s.qty AS Quantidade, s.unit_price AS Preço_unitário,
                               s.revenue AS Total, s.cogs AS Custo, s.revenue - s.cogs AS Lucro,
                               COALESCE(s.notes, '') AS Observação
                        FROM sales s
                        JOIN products p ON p.id = s.product_id
                        WHERE s.id = ? AND s.command_id = ?
                        """,
                        (int(sale_id), int(command_id)),
                    )

                for _, item in vendas_comanda.iterrows():
                    sale_id = int(item["ID"])
                    produto_nome = html.escape(str(item.get("Produto", "-")))
                    categoria = html.escape(str(item.get("Categoria", "-")))
                    sku = html.escape(str(item.get("SKU", "-") or "-"))
                    observacao = html.escape(str(item.get("Observação", "") or ""))
                    qtd = float(item.get("Quantidade", 0) or 0)
                    preco = float(item.get("Preço_unitário", 0) or 0)
                    total_item = float(item.get("Total", 0) or 0)
                    custo_item = float(item.get("Custo", 0) or 0)
                    lucro_item = float(item.get("Lucro", 0) or 0)

                    with st.container(border=True):
                        item_info_col, action_edit, action_delete = st.columns([8.2, 0.75, 0.85], vertical_alignment="bottom")
                        with item_info_col:
                            st.markdown(
                                f"""
                                <div class="command-item-card">
                                    <div class="command-item-title">{produto_nome}</div>
                                    <div class="command-item-meta">{categoria} • SKU: {sku}</div>
                                    <div class="command-item-row">
                                        <strong>Qtd:</strong> {qtd:g} &nbsp;|&nbsp;
                                        <strong>Preço:</strong> {brl(preco)} &nbsp;|&nbsp;
                                        <strong>Total:</strong> <span class="command-item-total">{brl(total_item)}</span>
                                    </div>
                                    <div class="command-item-row">
                                        <strong>Custo:</strong> {brl(custo_item)} &nbsp;|&nbsp;
                                        <strong>Lucro:</strong> {brl(lucro_item)}
                                    </div>
                                    <div class="command-item-notes"><strong>Observação:</strong> {observacao or '-'}</div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                        with action_edit:
                            if st.button("Editar", key=f"open_edit_sale_item_card_{command_id}_{sale_id}"):
                                st.session_state["edit_sale_item_card_id"] = sale_id
                                st.rerun()
                        with action_delete:
                            if st.button("Excluir", key=f"open_delete_sale_item_card_{command_id}_{sale_id}"):
                                st.session_state["delete_sale_item_card_id"] = sale_id
                                st.rerun()

                edit_sale_id = st.session_state.get("edit_sale_item_card_id")
                if edit_sale_id:
                    edit_df = _sale_item_details(edit_sale_id)
                    if edit_df.empty:
                        st.session_state.pop("edit_sale_item_card_id", None)
                    else:
                        edit_row = edit_df.iloc[0]

                        @st.dialog(f"Editar item #{int(edit_row['ID'])}")
                        def edit_sale_item_dialog():
                            all_products = product_options(active_only=False)
                            if all_products.empty:
                                st.warning("Nenhum produto cadastrado para edição.")
                                if st.button("Fechar", key=f"close_edit_sale_empty_{int(edit_row['ID'])}"):
                                    st.session_state.pop("edit_sale_item_card_id", None)
                                    st.rerun()
                                return

                            product_edit_map = {
                                f"{(str(r['sku']) + ' — ') if pd.notna(r['sku']) and str(r['sku']).strip() else ''}{r['name']} — {r['category']} — estoque: {float(r['stock_qty']):g} — preço: {brl(r['sale_price'])}": int(r["id"])
                                for _, r in all_products.iterrows()
                            }
                            product_id_to_label = {v: k for k, v in product_edit_map.items()}
                            product_labels = list(product_edit_map.keys())
                            current_product_id = int(edit_row["product_id"])
                            current_label = product_id_to_label.get(current_product_id, product_labels[0])
                            current_index = product_labels.index(current_label) if current_label in product_labels else 0

                            with st.form(f"form_edit_sale_item_card_{int(edit_row['ID'])}"):
                                product_label = st.selectbox("Produto", product_labels, index=current_index)
                                qty_edit = st.number_input("Quantidade", min_value=0.01, value=float(edit_row["Quantidade"] or 1), step=1.0, format="%.2f")
                                unit_price_edit = st.number_input("Preço unitário", min_value=0.0, value=float(edit_row["Preço_unitário"] or 0), step=0.10, format="%.2f")
                                notes_edit = st.text_input("Observação", value=str(edit_row.get("Observação", "") or ""))
                                c_save, c_cancel = st.columns(2)
                                save = c_save.form_submit_button("Salvar alteração", type="primary", width="stretch")
                                cancel = c_cancel.form_submit_button("Cancelar", width="stretch")

                            if save:
                                try:
                                    new_product_id = product_edit_map.get(product_label)
                                    if new_product_id is None:
                                        raise ValueError("Selecione um produto válido.")
                                    update_command_sale(int(edit_row["ID"]), int(new_product_id), float(qty_edit), float(unit_price_edit), notes_edit)
                                    st.session_state.pop("edit_sale_item_card_id", None)
                                    st.success("Item alterado e estoque ajustado automaticamente.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(str(e))
                            if cancel:
                                st.session_state.pop("edit_sale_item_card_id", None)
                                st.rerun()

                        edit_sale_item_dialog()

                delete_sale_id = st.session_state.get("delete_sale_item_card_id")
                if delete_sale_id:
                    delete_df = _sale_item_details(delete_sale_id)
                    if delete_df.empty:
                        st.session_state.pop("delete_sale_item_card_id", None)
                    else:
                        delete_row = delete_df.iloc[0]

                        @st.dialog(f"Excluir item #{int(delete_row['ID'])}")
                        def delete_sale_item_dialog():
                            st.warning(f"Deseja excluir {delete_row['Produto']} da comanda #{int(cmd['number'])}?")
                            st.caption("O estoque será devolvido automaticamente após a exclusão.")
                            c_cancel, c_delete = st.columns(2)
                            if c_cancel.button("Cancelar", width="stretch", key=f"cancel_delete_sale_card_{int(delete_row['ID'])}"):
                                st.session_state.pop("delete_sale_item_card_id", None)
                                st.rerun()
                            if c_delete.button("Excluir item", width="stretch", key=f"confirm_delete_sale_card_{int(delete_row['ID'])}"):
                                try:
                                    delete_command_sale(int(delete_row["ID"]))
                                    st.session_state.pop("delete_sale_item_card_id", None)
                                    st.success("Item excluído e estoque devolvido automaticamente.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(str(e))

                        delete_sale_item_dialog()

            c1, c2 = st.columns(2)
            if c1.button("Fechar comanda", type="primary"):
                st.session_state["show_close_command_dialog"] = command_id
                st.rerun()

            if st.session_state.get("show_close_command_dialog") == command_id:
                @st.dialog(f"Finalizar comanda #{int(cmd['number'])}", width="large")
                def close_dialog():
                    st.markdown(
                        """
                        <style>
                        div[data-testid="stDialog"] div[role="dialog"] {
                            width: min(960px, 96vw) !important;
                            max-width: 960px !important;
                        }
                        div[data-testid="stDialog"] div[role="dialog"] > div {
                            padding-left: 1.2rem !important;
                            padding-right: 1.2rem !important;
                        }
                        div[data-testid="stDialog"] [data-testid="stMetricValue"] {
                            font-size: 2rem !important;
                            line-height: 1.1 !important;
                            white-space: normal !important;
                            word-break: break-word !important;
                        }
                        div[data-testid="stDialog"] .stButton > button,
                        div[data-testid="stDialog"] .stFormSubmitButton > button {
                            width: 100%;
                        }
                        @media (max-width: 768px) {
                            div[data-testid="stDialog"] div[role="dialog"] {
                                width: 96vw !important;
                                max-width: 96vw !important;
                            }
                            div[data-testid="stDialog"] [data-testid="stMetricValue"] {
                                font-size: 1.65rem !important;
                            }
                            div[data-testid="stDialog"] h1, div[data-testid="stDialog"] h2, div[data-testid="stDialog"] h3 {
                                font-size: 1.45rem !important;
                            }
                        }
                        </style>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.markdown("**Resumo da comanda antes do fechamento**")
                    st.caption(f"Comanda #{int(cmd['number'])} • {cmd['operator_name']} • {cmd['event_name']}")

                    entrada_base = float(entrada_original if entrada_original > 0 else entrada)
                    st.write("**Entrada**")
                    entrada_cortesia_final = st.checkbox(
                        "Aplicar cortesia na entrada desta comanda",
                        value=entrada_cortesia,
                        disabled=(str(cmd['entry_type'] or '') == "Sem entrada" or entrada_base <= 0),
                        key=f"close_entry_courtesy_{command_id}",
                    )
                    entrada_cortesia_motivo_final = entrada_cortesia_motivo
                    if entrada_cortesia_final:
                        entrada_cortesia_motivo_final = st.text_input(
                            "Motivo da cortesia da entrada (opcional)",
                            value=entrada_cortesia_motivo,
                            placeholder="Ex.: Convidado da organização, staff, influenciador...",
                            key=f"close_entry_courtesy_reason_{command_id}",
                        )
                    entrada_para_fechamento = 0.0 if entrada_cortesia_final else entrada_base
                    cortesia_linha = ""
                    if entrada_cortesia_final:
                        cortesia_linha = (
                            f'<div style="color:#595541; margin-top:4px;">'
                            f'Valor original: <strong>{brl(entrada_base)}</strong> | '
                            f'Cortesia aplicada: <strong>-{brl(entrada_base)}</strong> | '
                            f'Motivo: {html.escape(entrada_cortesia_motivo_final or "-")}'
                            f'</div>'
                        )

                    st.markdown(
                        f"""
                        <div style="border:1px solid #c7b58d; border-left:4px solid #314132; padding:10px 12px; margin:8px 0; background:#f5ecd9;">
                            <div style="font-weight:900; color:#263428; text-transform:uppercase;">Entrada — {html.escape(str(cmd['entry_type'] or '-'))}</div>
                            <div style="color:#1f261f; margin-top:4px;">Valor cobrado: <strong>{brl(entrada_para_fechamento)}</strong></div>
                            {cortesia_linha}
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                    if vendas_comanda.empty:
                        st.info("Nenhum produto/consumo lançado nesta comanda. O fechamento será apenas com o valor da entrada.")
                    else:
                        for _, resumo_item in vendas_comanda.iterrows():
                            produto_resumo = html.escape(str(resumo_item.get("Produto", "-")))
                            categoria_resumo = html.escape(str(resumo_item.get("Categoria", "-") or "-"))
                            sku_resumo = html.escape(str(resumo_item.get("SKU", "-") or "-"))
                            obs_resumo = html.escape(str(resumo_item.get("Observação", "") or ""))
                            qtd_resumo = float(resumo_item.get("Quantidade", 0) or 0)
                            preco_resumo = float(resumo_item.get("Preço_unitário", 0) or 0)
                            total_resumo = float(resumo_item.get("Total", 0) or 0)
                            st.markdown(
                                f"""
                                <div style="border:1px solid #c7b58d; border-left:4px solid #314132; padding:10px 12px; margin:8px 0; background:#f5ecd9;">
                                    <div style="font-weight:900; color:#263428; text-transform:uppercase;">{produto_resumo}</div>
                                    <div style="color:#595541; font-size:.9rem;">{categoria_resumo} • SKU: {sku_resumo}</div>
                                    <div style="color:#1f261f; margin-top:4px;">{qtd_resumo:g} x {brl(preco_resumo)} = <strong>{brl(total_resumo)}</strong></div>
                                    <div style="color:#595541; font-size:.9rem; margin-top:2px;">Observação: {obs_resumo or '-'}</div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

                    resumo_cols = st.columns(3)
                    resumo_cols[0].metric("Entrada", brl(entrada_para_fechamento))
                    resumo_cols[1].metric("Vendas/consumo", brl(consumo))
                    resumo_cols[2].metric("Subtotal", brl(entrada_para_fechamento + consumo))

                    st.divider()
                    subtotal_dialog = entrada_para_fechamento + consumo
                    payment_method = st.selectbox("Forma de pagamento", FORMAS_PAGAMENTO)
                    st.write("Deseja aplicar algum desconto nesta comanda?")
                    desconto_percent = st.number_input(
                        "Desconto (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=0.0,
                        step=1.0,
                        format="%.2f",
                    )
                    desconto_valor = subtotal_dialog * desconto_percent / 100.0
                    total_final = subtotal_dialog - desconto_valor
                    cA, cB, cC = st.columns(3)
                    cA.metric("Subtotal", brl(subtotal_dialog))
                    cB.metric("Desconto", brl(desconto_valor))
                    cC.metric("Total final", brl(total_final))
                    cc1, cc2 = st.columns(2)
                    if cc1.button("Confirmar fechamento", type="primary"):
                        try:
                            close_command(command_id, desconto_percent, payment_method, entrada_cortesia_final, entrada_cortesia_motivo_final)
                            st.session_state.pop("show_close_command_dialog", None)
                            st.success("Comanda fechada com sucesso e recebimento lançado no caixa.")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
                    if cc2.button("Voltar sem fechar"):
                        st.session_state.pop("show_close_command_dialog", None)
                        st.rerun()
                close_dialog()

            if c2.button("Cancelar comanda sem vendas"):
                try:
                    cancel_command(command_id)
                    st.success("Comanda cancelada.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    with tab_history:
        filtro = st.radio("Status", ["Todas", "Aberta", "Fechada", "Cancelada"], horizontal=True)
        df = command_options(None if filtro == "Todas" else filtro)
        show = df.rename(columns={"number": "Comanda", "status": "Status", "opened_at": "Data", "event_name": "Jogo / evento", "operator_name": "Operador / jogador", "customer_name": "Jogador / cliente", "entry_type": "Tipo de entrada", "entry_value": "Valor da entrada", "subtotal": "Subtotal", "discount_percent": "Desconto %", "discount_amount": "Valor do desconto", "total": "Total final"})
        pretty_dataframe(format_date_columns(show, columns=("Data",)), width="stretch", hide_index=True)
        csv_download(show, "comandas.csv", "Baixar comandas em CSV")
        fechadas = command_options("Fechada")
        if not fechadas.empty:
            reopen_map = {f"#{int(r['number'])} — {r['customer_name'] or 'sem nome'} — {brl(r['total'])}": int(r["id"]) for _, r in fechadas.iterrows()}
            selected = st.selectbox("Reabrir comanda fechada", ["Selecione"] + list(reopen_map.keys()))
            if st.button("Reabrir comanda selecionada"):
                if selected == "Selecione":
                    st.warning("Selecione uma comanda.")
                else:
                    reopen_command(reopen_map[selected])
                    st.success("Comanda reaberta.")
                    st.rerun()

def page_events_operation():
    st.subheader("Jogos / Operação")
    st.caption("Crie o jogo aqui. Depois, opere as comandas pela aba Comandas. As comandas alimentam automaticamente jogadores, entradas, vendas, custos e lucro do jogo.")

    if st.session_state.get("flash_jogo_criado"):
        st.success(st.session_state.pop("flash_jogo_criado"))

    tab_options = ["1. Criar jogo", "2. Resumo e despesas do jogo", "3. Histórico e resultados"]
    # Não altere diretamente st.session_state["events_tab_choice"] depois que o widget já foi criado.
    # Para navegar programaticamente, usamos uma chave auxiliar e aplicamos antes de instanciar o radio.
    if "events_tab_choice_next" in st.session_state:
        current_tab = st.session_state.pop("events_tab_choice_next")
        st.session_state["events_tab_choice"] = current_tab
    else:
        current_tab = st.session_state.get("events_tab_choice", "1. Criar jogo")
    if current_tab not in tab_options:
        current_tab = "1. Criar jogo"
    selected_tab = st.radio(
        "Etapa",
        tab_options,
        horizontal=True,
        index=tab_options.index(current_tab),
        key="events_tab_choice",
        label_visibility="collapsed",
    )

    if selected_tab == "1. Criar jogo":
        st.write("**Criar novo jogo/evento**")
        open_cash_session = get_open_cash_session()
        if open_cash_session is None:
            st.error("Para criar/abrir um jogo, primeiro abra o caixa. Vá em Caixa > Caixa atual e clique em Abrir caixa.")
            st.info("Essa regra evita criar jogos sem controle financeiro ativo para comandas, recebimentos e fechamento do dia.")
            return
        st.success(f"Caixa aberto. Saldo esperado atual: {brl(cash_expected_amount(int(open_cash_session['id'])))}")
        open_events_df = get_open_events_df()
        if not open_events_df.empty:
            jogo_aberto = open_events_df.iloc[0]
            st.error("Não é possível criar um novo jogo enquanto houver jogo em aberto.")
            st.info(f"Jogo aberto atual: {format_date_br(jogo_aberto['event_date'])} — {jogo_aberto['name']}. Finalize este jogo na aba 'Resumo e despesas do jogo' antes de criar outro.")
            return
        nonce = st.session_state.get("event_create_form_nonce", 0)
        with st.form(f"form_event_unified_create_v29_{nonce}", clear_on_submit=True):
            c1, c2 = st.columns(2)
            event_date = c1.date_input("Data", value=date.today(), format="DD/MM/YYYY", key=f"event_create_date_v29_{nonce}")
            name = c2.text_input("Nome do jogo/evento", value="", key=f"event_create_name_v29_{nonce}")
            st.write("**Valores padrão para as comandas deste jogo**")
            c3, c4 = st.columns(2)
            rental_unit_price_text = c3.text_input(
                "Entrada com aluguel",
                value="",
                placeholder="Ex: 150,00",
                key=f"event_create_rental_price_v63_{nonce}",
            )
            own_equipment_unit_price_text = c4.text_input(
                "Entrada com equipamento próprio",
                value="",
                placeholder="Ex: 50,00",
                key=f"event_create_own_price_v63_{nonce}",
            )
            notes = st.text_area("Observações", key=f"event_create_notes_v29_{nonce}")
            if st.form_submit_button("Criar jogo/evento"):
                rental_unit_price = parse_money_input(rental_unit_price_text)
                own_equipment_unit_price = parse_money_input(own_equipment_unit_price_text)
                if not name.strip():
                    st.error("Informe o nome do jogo/evento.")
                elif get_open_cash_session() is None:
                    st.error("Abra o caixa antes de criar/abrir o jogo.")
                elif count_open_events() > 0:
                    st.error("Não é possível criar um novo jogo enquanto houver jogo em aberto. Finalize o jogo atual primeiro.")
                else:
                    event_id = execute(
                        """
                        INSERT INTO events (event_date, name, rental_unit_price, own_equipment_unit_price, notes)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (str(event_date), name.strip(), float(rental_unit_price), float(own_equipment_unit_price), notes),
                    )
                    log_action(
                        "criou_jogo",
                        f"Jogo criado: {name.strip()} | Data: {format_date_br(event_date)} | Entrada aluguel: {brl(rental_unit_price)} | Entrada equipamento próprio: {brl(own_equipment_unit_price)} | ID: {event_id}",
                    )
                    st.session_state["flash_jogo_criado"] = "Jogo criado com sucesso. Agora vá para a aba Comandas para abrir as comandas deste jogo."
                    st.session_state["event_create_form_nonce"] = nonce + 1
                    st.session_state["events_tab_choice_next"] = "2. Resumo e despesas do jogo"
                    st.rerun()

    elif selected_tab == "2. Resumo e despesas do jogo":
        events = event_options()
        if events.empty:
            st.info("Crie um jogo/evento na primeira aba para começar.")
            return

        event_labels = [f"{format_date_br(r['event_date'])} — {r['name']}" for _, r in events.iterrows()]
        event_ids = {f"{format_date_br(r['event_date'])} — {r['name']}": int(r["id"]) for _, r in events.iterrows()}
        event_label = st.selectbox("Jogo em operação", event_labels, key="event_oper_select_v12")
        event_id = event_ids[event_label]

        sync_event_from_commands(event_id)
        event = query_df("SELECT * FROM events WHERE id = ?", (event_id,)).iloc[0]

        fin = get_event_financials(event_id)
        receita_produtos = fin["receita_produtos"]
        descontos_comandas = fin.get("descontos_comandas", 0)
        custo_produtos = fin["custo_produtos"]
        despesas_fixas = fin["despesas_fixas"]
        despesas_variaveis = fin["despesas_variaveis"]
        receita_entradas = float(event["entry_revenue"])
        total_receita = receita_entradas + receita_produtos - descontos_comandas
        total_jogadores = int(event["players"])
        lucro_antes_fixas = total_receita - custo_produtos - despesas_variaveis
        lucro_final = lucro_antes_fixas - despesas_fixas
        ticket = total_receita / total_jogadores if total_jogadores else 0
        custo_variavel_por_jogador = despesas_variaveis / total_jogadores if total_jogadores else 0

        rental_qty = int(event["rental_qty"])
        own_qty = int(event["own_equipment_qty"])
        comandos_abertas_qtd = int(query_df("SELECT COUNT(*) AS total FROM commands WHERE event_id = ? AND status = 'Aberta'", (event_id,))["total"].iloc[0])

        st.write("**Detalhes cadastrados do jogo**")
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Status do jogo", str(event.get("status", "Aberto")))
        d2.metric("Entrada com aluguel", brl(float(event["rental_unit_price"])))
        d3.metric("Entrada com equipamento próprio", brl(float(event["own_equipment_unit_price"])))
        d4.metric("Data do jogo", format_date_br(event["event_date"]))
        if pd.notna(event.get("notes", None)) and str(event.get("notes", "")).strip():
            st.info(f"Observações: {event['notes']}")

        st.write("**Resumo automático do jogo**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Jogadores com aluguel", rental_qty)
        c2.metric("Jogadores equip. próprio", own_qty)
        c3.metric("Total de jogadores", total_jogadores)
        c4.metric("Comandas abertas", comandos_abertas_qtd)

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Receita de entradas", brl(receita_entradas))
        c6.metric("Receita de produtos", brl(receita_produtos))
        c7.metric("Descontos", brl(descontos_comandas))
        c8.metric("Receita total", brl(total_receita))

        c9, c10, c11, c12 = st.columns(4)
        c9.metric("Custo produtos", brl(custo_produtos))
        c10.metric("Desp. variáveis", brl(despesas_variaveis))
        c11.metric("Custo variável/jogador", brl(custo_variavel_por_jogador))
        c12.metric("Lucro final", brl(lucro_final))

        st.divider()
        st.info("Para abrir, lançar consumo, fechar ou cancelar comandas deste jogo, use a aba **Comandas** no menu principal. Esta tela fica apenas para acompanhar o resumo e lançar despesas do jogo.")

        st.divider()
        st.write("**Resumo de vendas por categoria**")
        vendas_categoria_df = query_df(
            """
            SELECT p.category AS Categoria,
                   SUM(s.qty) AS Quantidade_vendida,
                   SUM(s.revenue) AS Receita,
                   SUM(s.cogs) AS Custo,
                   SUM(s.revenue - s.cogs) AS Lucro_bruto
            FROM sales s
            JOIN products p ON p.id = s.product_id
            WHERE s.event_id = ?
            GROUP BY p.category
            ORDER BY SUM(s.revenue) DESC
            """,
            (event_id,),
        )
        if vendas_categoria_df.empty:
            st.info("Nenhuma venda/consumo lançada neste jogo ainda.")
        else:
            pretty_dataframe(vendas_categoria_df, width="stretch", hide_index=True)

        produtos_vendidos_df = query_df(
            """
            SELECT p.category AS Categoria, p.name AS Produto,
                   SUM(s.qty) AS Quantidade_vendida,
                   SUM(s.revenue) AS Receita,
                   SUM(s.cogs) AS Custo,
                   SUM(s.revenue - s.cogs) AS Lucro_bruto
            FROM sales s
            JOIN products p ON p.id = s.product_id
            WHERE s.event_id = ?
            GROUP BY p.category, p.name
            ORDER BY p.category, SUM(s.qty) DESC, p.name
            """,
            (event_id,),
        )
        with st.expander("Ver produtos vendidos no jogo"):
            if produtos_vendidos_df.empty:
                st.info("Nenhum produto vendido.")
            else:
                pretty_dataframe(produtos_vendidos_df, width="stretch", hide_index=True)

        st.divider()
        st.write("**Despesas do jogo**")
        st.caption("Cadastre uma nova despesa abaixo. Ao digitar o valor, você pode apertar Enter para registrar ou clicar no botão.")
        operadores_exp = operators_options()
        op_exp_map = {str(r["name"]): int(r["id"]) for _, r in operadores_exp.iterrows()}
        with st.form("form_event_expense_v59", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            category = c1.selectbox("Categoria", ["Funcionário", "Aluguel", "Compra operacional", "Manutenção", "Marketing", "Outro"], key="expense_cat_v59")
            expense_type = c2.radio("Tipo", ["Fixa", "Variável"], horizontal=True, key="expense_type_v59")
            op_exp_label = c3.selectbox("Operador/jogador relacionado", list(op_exp_map.keys()), key="expense_operator_v59") if op_exp_map else None
            description = st.text_input("Descrição", value="", key="expense_desc_v59")
            amount_text = st.text_input(
                "Valor",
                value="",
                placeholder="Ex: 120,00",
                key="expense_amount_text_v59",
            )
            amount = parse_money_input(amount_text)
            if expense_type == "Variável" and total_jogadores > 0 and amount > 0:
                st.info(f"Essa despesa representa {brl(float(amount) / total_jogadores)} por jogador.")
            if st.form_submit_button("Registrar despesa"):
                if amount <= 0:
                    st.error("Informe um valor maior que zero.")
                else:
                    notes = f"TIPO_DESPESA={expense_type.upper().replace('Á', 'A')}; Lançado em Jogos / Operação"
                    execute(
                        "INSERT INTO expenses (expense_date, category, description, amount, event_id, operator_id, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (str(date.today()), category, description.strip() or "Despesa do jogo", amount, event_id, op_exp_map.get(op_exp_label) if op_exp_label else None, notes),
                    )
                    st.success("Despesa registrada no jogo.")
                    st.rerun()

        despesas_df = query_df(
            f"""
            SELECT
                ex.id AS ID,
                ex.expense_date AS Data,
                {expense_type_sql('ex')} AS Tipo,
                ex.category AS Categoria,
                ex.description AS Descrição,
                ex.amount AS Valor,
                ex.operator_id AS Operador_ID,
                CASE WHEN {expense_type_sql('ex')} = 'Variável' AND ? > 0 THEN ex.amount / ? ELSE 0 END AS Custo_variável_por_jogador
            FROM expenses ex
            WHERE ex.event_id = ?
            ORDER BY ex.id DESC
            """,
            (total_jogadores, total_jogadores, event_id),
        )
        if despesas_df.empty:
            st.info("Nenhuma despesa lançada neste jogo.")
        else:
            st.write("**Editar ou excluir despesas lançadas**")
            st.caption("Edite as despesas diretamente na tabela abaixo. Para excluir, marque a coluna Selecionar e clique em Excluir despesas selecionadas.")

            op_editor_id_by_label = {"-": None}
            op_editor_label_by_id = {None: "-"}
            for _, op_row in operadores_exp.iterrows():
                op_label = str(op_row["name"])
                op_editor_id_by_label[op_label] = int(op_row["id"])
                op_editor_label_by_id[int(op_row["id"])] = op_label

            despesas_view = despesas_df.copy()
            despesas_view["Selecionar"] = False
            despesas_view["Operador_jogador"] = despesas_view["Operador_ID"].apply(lambda x: op_editor_label_by_id.get(int(x), "-") if pd.notna(x) else "-")
            despesas_view = despesas_view[["Selecionar", "ID", "Data", "Tipo", "Categoria", "Descrição", "Valor", "Operador_jogador", "Custo_variável_por_jogador"]]

            edited_expenses = st.data_editor(
                format_date_columns(despesas_view),
                width="stretch",
                hide_index=True,
                disabled=["ID", "Data", "Custo_variável_por_jogador"],
                column_config={
                    "Selecionar": st.column_config.CheckboxColumn("Selecionar"),
                    "Tipo": st.column_config.SelectboxColumn("Tipo", options=["Fixa", "Variável"], required=True),
                    "Categoria": st.column_config.SelectboxColumn("Categoria", options=["Funcionário", "Aluguel", "Compra operacional", "Manutenção", "Marketing", "Outro"], required=True),
                    "Operador_jogador": st.column_config.SelectboxColumn("Operador/jogador", options=list(op_editor_id_by_label.keys()), required=False),
                    "Valor": st.column_config.NumberColumn("Valor", min_value=0.01, step=10.0, format="R$ %.2f"),
                    "Custo_variável_por_jogador": st.column_config.NumberColumn("Custo variável por jogador", format="R$ %.2f"),
                },
                key=f"expenses_editor_v52_{event_id}",
            )

            b1, b2 = st.columns([1, 1])
            if b1.button("Salvar alterações de despesas", key=f"save_expense_changes_v52_{event_id}"):
                try:
                    changes = 0
                    original_by_id = despesas_df.set_index("ID")
                    for _, row in edited_expenses.iterrows():
                        expense_id_edit = int(row["ID"])
                        old_row = original_by_id.loc[expense_id_edit]
                        new_tipo = str(row["Tipo"])
                        new_categoria = str(row["Categoria"])
                        new_descricao = str(row["Descrição"])
                        new_valor = float(row["Valor"])
                        new_operator_id = op_editor_id_by_label.get(str(row.get("Operador_jogador", "-")))
                        old_operator_id = int(old_row["Operador_ID"]) if pd.notna(old_row["Operador_ID"]) else None
                        if (
                            new_tipo != str(old_row["Tipo"])
                            or new_categoria != str(old_row["Categoria"])
                            or new_descricao != str(old_row["Descrição"])
                            or abs(new_valor - float(old_row["Valor"])) > 0.0001
                            or new_operator_id != old_operator_id
                        ):
                            update_expense(expense_id_edit, new_categoria, new_descricao, new_valor, new_tipo, new_operator_id)
                            changes += 1
                    if changes:
                        st.success(f"{changes} despesa(s) alterada(s) com sucesso.")
                        st.rerun()
                    else:
                        st.info("Nenhuma alteração encontrada.")
                except Exception as e:
                    st.error(str(e))

            selected_expenses = edited_expenses[edited_expenses["Selecionar"] == True] if "Selecionar" in edited_expenses.columns else pd.DataFrame()
            if b2.button("Excluir despesas selecionadas", key=f"delete_expense_v52_{event_id}"):
                if selected_expenses.empty:
                    st.warning("Selecione pelo menos uma despesa para excluir.")
                else:
                    for _, row in selected_expenses.iterrows():
                        delete_expense(int(row["ID"]))
                    st.success("Despesa(s) excluída(s).")
                    st.rerun()

        st.divider()
        st.markdown("""
        <style>
        div[data-testid="stButton"] button[aria-label="Excluir jogo"] {
            background-color: #d92d20 !important;
            color: #ffffff !important;
            border: 1px solid #d92d20 !important;
            border-radius: 6px !important;
            padding: 0.25rem 0.75rem !important;
            min-height: 30px !important;
            height: 30px !important;
            font-size: 0.85rem !important;
            line-height: 1 !important;
        }
        div[data-testid="stButton"] button[aria-label="Excluir jogo"] p {
            color: #ffffff !important;
            font-size: 0.85rem !important;
        }
        div[data-testid="stButton"] button[aria-label="Excluir jogo"]:hover {
            background-color: #b42318 !important;
            border-color: #b42318 !important;
            color: #ffffff !important;
        }
        </style>
        """, unsafe_allow_html=True)

        _, action_area, _ = st.columns([1.5, 2, 1.5])
        with action_area:
            st.caption("Ações do jogo")
            a1, a2 = st.columns(2)
            with a1:
                if st.button("Finalizar jogo", key=f"open_finalize_event_popup_{event_id}", type="primary", width="stretch", disabled=str(event.get("status", "Aberto")) == "Fechado"):
                    show_finalize_event_dialog(event_id)
            with a2:
                if st.button("Excluir jogo", key=f"open_delete_event_popup_{event_id}", type="primary", width="stretch"):
                    show_delete_event_dialog(event_id)

    elif selected_tab == "3. Histórico e resultados":
        st.write("**Histórico e resultado por jogo/evento**")
        tipo_ex = expense_type_sql("ex")
        df = query_df(
            f"""
            SELECT e.id, e.event_date AS Data, e.name AS Evento,
                   COALESCE(e.status, 'Aberto') AS Status,
                   e.rental_qty AS Qtd_aluguéis,
                   e.own_equipment_qty AS Qtd_equipamento_próprio,
                   e.players AS Total_jogadores,
                   e.entry_revenue AS Total_entradas,
                   COALESCE(SUM(s.revenue), 0) AS Receita_produtos,
                   COALESCE(SUM(s.cogs), 0) AS Custo_produtos,
                   COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id AND {tipo_ex} = 'Variável'), 0) AS Despesas_variáveis,
                   COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id AND {tipo_ex} = 'Fixa'), 0) AS Despesas_fixas,
                   CASE WHEN e.players > 0 THEN COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id AND {tipo_ex} = 'Variável'), 0) / e.players ELSE 0 END AS Custo_variável_por_jogador,
                   COALESCE((SELECT SUM(discount_amount) FROM commands c WHERE c.event_id = e.id AND c.status = 'Fechada'), 0) AS Descontos,
                   (e.entry_revenue + COALESCE(SUM(s.revenue), 0) - COALESCE((SELECT SUM(discount_amount) FROM commands c WHERE c.event_id = e.id AND c.status = 'Fechada'), 0)) AS Receita_total,
                   (e.entry_revenue + COALESCE(SUM(s.revenue), 0) - COALESCE(SUM(s.cogs), 0) - COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id), 0) - COALESCE((SELECT SUM(discount_amount) FROM commands c WHERE c.event_id = e.id AND c.status = 'Fechada'), 0)) AS Lucro_final
            FROM events e
            LEFT JOIN sales s ON s.event_id = e.id
            GROUP BY e.id
            ORDER BY e.event_date DESC, e.id DESC
            """
        )
        pretty_dataframe(format_date_columns(df), width="stretch", hide_index=True)
        csv_download(df, "resultado_jogos_eventos.csv", "Baixar resultado dos jogos em CSV")

def page_stock_movements():
    st.subheader("Movimentações de estoque")
    st.caption("Use esta tela para perdas, consumo interno, ajustes de contagem, devoluções e outras movimentações que não sejam compra/venda.")
    products = product_options()
    if products.empty:
        st.info("Cadastre um produto primeiro.")
        return

    events = event_options()
    event_labels = ["Sem vincular a jogo/evento"] + [f"{format_date_br(r['event_date'])} — {r['name']}" for _, r in events.iterrows()]
    event_ids = {"Sem vincular a jogo/evento": None}
    event_ids.update({f"{format_date_br(r['event_date'])} — {r['name']}": int(r["id"]) for _, r in events.iterrows()})
    product_map = {f"{(r['sku'] + ' — ') if pd.notna(r['sku']) and str(r['sku']).strip() else ''}{r['name']} — estoque: {r['stock_qty']:g}": int(r["id"]) for _, r in products.iterrows()}

    with st.form("form_stock_movement"):
        c1, c2 = st.columns(2)
        movement_date = c1.date_input("Data", value=date.today(), format="DD/MM/YYYY")
        event_label = c2.selectbox("Jogo/evento", event_labels)
        product_label = st.selectbox("Produto", list(product_map.keys()))
        movement_type = st.selectbox("Tipo", TIPOS_MOVIMENTACAO)
        c3, c4 = st.columns(2)
        qty_abs = c3.number_input("Quantidade", min_value=0.01, step=1.0)
        with c4:
            render_field_tooltip("Custo unitário de referência", "Se deixar 0, o sistema usa o custo médio atual do produto.")
            unit_cost = st.number_input("Custo unitário de referência", min_value=0.0, step=0.10, format="%.2f", label_visibility="collapsed")
        direction = st.radio("Essa movimentação entra ou sai do estoque?", ["Sai do estoque", "Entra no estoque"], horizontal=True)
        notes = st.text_area("Observações")
        if st.form_submit_button("Registrar movimentação"):
            try:
                qty = float(qty_abs) if direction == "Entra no estoque" else -float(qty_abs)
                add_stock_adjustment(movement_date, product_map[product_label], event_ids[event_label], movement_type, qty, unit_cost, notes)
                st.success("Movimentação registrada e estoque atualizado.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    df = query_df(
        """
        SELECT sm.movement_date AS Data, COALESCE(e.name, '-') AS Jogo_evento, p.sku AS SKU, p.name AS Produto,
               p.category AS Categoria, sm.movement_type AS Tipo, sm.qty AS Quantidade,
               sm.unit_cost AS Custo_unitário, sm.total_cost AS Custo_total, sm.unit_price AS Preço_unitário,
               sm.total_revenue AS Receita, sm.notes AS Observações
        FROM stock_movements sm
        JOIN products p ON p.id = sm.product_id
        LEFT JOIN events e ON e.id = sm.event_id
        ORDER BY sm.movement_date DESC, sm.id DESC
        """
    )
    st.write("**Histórico completo de movimentações**")
    pretty_dataframe(format_date_columns(df), width="stretch", hide_index=True)

def page_stock():
    st.subheader("Estoque")
    st.caption("Área centralizada para lançar compras/entradas e consultar movimentações de estoque. O cadastro de produtos fica no menu Produtos.")
    tab1, tab2 = st.tabs(["Entrada de Estoque", "Movimentações"])
    with tab1:
        page_stock_entries()
    with tab2:
        page_stock_movements()


def page_products():
    st.subheader("Produtos")
    with st.expander("Cadastrar novo produto", expanded=True):
        product_nonce = st.session_state.get("product_create_form_nonce", 0)
        with st.form(f"form_product_v29_{product_nonce}", clear_on_submit=True):
            c0, c1, c2, c3 = st.columns(4)
            sku = c0.text_input("SKU", placeholder="Ex: BEB-AGUA-500", key=f"product_sku_v29_{product_nonce}")
            name = c1.text_input("Nome do produto", key=f"product_name_v29_{product_nonce}")
            category = c2.selectbox("Categoria", CATEGORIAS_PADRAO, key=f"product_category_v29_{product_nonce}")
            unit = c3.text_input("Unidade", value="", placeholder="Ex: UN", key=f"product_unit_v29_{product_nonce}")

            f0, f1, f2 = st.columns(3)
            barcode = f0.text_input("Código de barras", placeholder="Somente números", key=f"product_barcode_v119_{product_nonce}")
            ncm = f1.text_input("NCM", placeholder="9 dígitos", max_chars=9, key=f"product_ncm_v119_{product_nonce}")
            cest = f2.text_input("Código CEST", placeholder="7 dígitos", max_chars=7, key=f"product_cest_v119_{product_nonce}")

            c4, c5, c6, c7 = st.columns(4)
            initial_stock_text = c4.text_input("Estoque inicial", value="", placeholder="Ex: 10", key=f"product_initial_stock_v119_{product_nonce}")
            min_stock_text = c5.text_input("Estoque mínimo", value="", placeholder="Ex: 3", key=f"product_min_stock_v119_{product_nonce}")
            cost_unit_text = c6.text_input("Custo unitário", value="", placeholder="Ex: 5,50", key=f"product_cost_v29_{product_nonce}")
            sale_price_text = c7.text_input("Preço de venda", value="", placeholder="Ex: 12,00", key=f"product_sale_v29_{product_nonce}")
            st.caption("O estoque mínimo também pode ser alterado depois pelo menu Estoque. As entradas futuras devem ser registradas no menu Estoque.")
            submitted = st.form_submit_button("Salvar produto")
            if submitted:
                ncm_clean = only_digits(ncm, 9)
                cest_clean = only_digits(cest, 7)
                barcode_clean = only_digits(barcode)
                if not name.strip():
                    st.error("Informe o nome do produto.")
                elif ncm_clean and len(ncm_clean) != 9:
                    st.error("O campo NCM deve conter exatamente 9 dígitos.")
                elif cest_clean and len(cest_clean) != 7:
                    st.error("O campo CEST deve conter exatamente 7 dígitos.")
                else:
                    initial_stock = parse_money_input(initial_stock_text)
                    min_stock = parse_money_input(min_stock_text)
                    cost_unit = parse_money_input(cost_unit_text)
                    sale_price = parse_money_input(sale_price_text)
                    if any(v < 0 for v in [initial_stock, min_stock, cost_unit, sale_price]):
                        st.error("Estoque, mínimo, custo e preço não podem ser negativos.")
                    else:
                        try:
                            new_product_id = execute(
                                """
                                INSERT INTO products (sku, barcode, ncm, cest, name, category, unit, stock_qty, min_stock, cost_unit, sale_price)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (sku.strip() or None, barcode_clean or None, ncm_clean or None, cest_clean or None, name.strip(), category, unit.strip() or "un", initial_stock, min_stock, cost_unit, sale_price),
                            )
                            if initial_stock > 0:
                                with get_conn() as conn:
                                    record_stock_movement(conn, date.today(), int(new_product_id), None, "Estoque inicial", float(initial_stock), float(cost_unit), 0, "Estoque inicial informado no cadastro do produto")
                                    conn.commit()
                            log_action(
                                "produto_criado",
                                f"Produto criado | ID: {new_product_id} | SKU: {sku.strip() or '-'} | Código de barras: {barcode_clean or '-'} | NCM: {ncm_clean or '-'} | CEST: {cest_clean or '-'} | Nome: {name.strip()} | Categoria: {category} | Unidade: {unit.strip() or 'un'} | Estoque inicial: {initial_stock:g} | Estoque mínimo: {min_stock:g} | Custo padrão: {brl(cost_unit)} | Preço de venda: {brl(sale_price)}",
                            )
                            st.session_state["product_create_form_nonce"] = product_nonce + 1
                            st.success("Produto cadastrado.")
                            st.rerun()
                        except DB_INTEGRITY_ERROR:
                            st.error("Já existe um produto com esse nome ou SKU.")

    st.write("**Produtos cadastrados**")
    visualizar = st.radio(
        "Visualização",
        ["Produtos ativos", "Produtos excluídos/inativos", "Todos os produtos"],
        horizontal=True,
        key="products_view_mode",
    )
    f_search, f_category = st.columns([2, 1])
    busca_produto = f_search.text_input("Pesquisar produto", placeholder="Busque por nome, SKU, código de barras, NCM, CEST ou categoria", key="product_search_v119")
    categoria_filtro = f_category.selectbox("Filtrar categoria", ["Todas"] + CATEGORIAS_PADRAO, key="product_category_filter_v119")

    where_parts = []
    params = []
    if visualizar == "Produtos ativos":
        where_parts.append("active = 1")
    elif visualizar == "Produtos excluídos/inativos":
        where_parts.append("active = 0")
    if categoria_filtro != "Todas":
        where_parts.append("category = ?")
        params.append(categoria_filtro)
    busca_normalizada = normalize_search_text(busca_produto)
    filtro_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    df = query_df(
        f"""
        SELECT id, sku AS SKU, barcode AS Codigo_barras, ncm AS NCM, cest AS CEST,
               name AS Produto, category AS Categoria, unit AS Unidade, stock_qty AS Estoque,
               min_stock AS Mínimo, cost_unit AS Custo_unitário, sale_price AS Preço_venda,
               CASE WHEN active = 1 THEN 'Ativo' ELSE 'Excluído/Inativo' END AS Status
        FROM products
        {filtro_sql}
        ORDER BY active DESC, category, name
        """,
        params,
    )

    if busca_normalizada and not df.empty:
        search_cols = ["Produto", "SKU", "Codigo_barras", "NCM", "CEST", "Categoria", "Unidade", "Status"]
        def row_matches_search(row):
            haystack = " ".join(normalize_search_text(row.get(col, "")) for col in search_cols)
            return busca_normalizada in haystack
        df = df[df.apply(row_matches_search, axis=1)]

    if df.empty:
        if busca_normalizada:
            st.info("Nenhum produto encontrado para a pesquisa informada.")
        elif visualizar == "Produtos excluídos/inativos":
            st.info("Nenhum produto excluído/inativo encontrado. Obs.: a partir desta versão, os produtos excluídos ficam guardados aqui para consulta.")
        else:
            st.info("Nenhum produto cadastrado ainda.")
        return

    st.caption("Produtos exibidos em cards. Use as ações do card para alterar, inativar ou restaurar.")

    produtos = list(df.iterrows())
    for row_start in range(0, len(produtos), 2):
        row_cols = st.columns(2)
        for col_index, (_, produto) in enumerate(produtos[row_start:row_start + 2]):
            with row_cols[col_index]:
                produto_id = int(produto["id"])
                nome = html.escape(str(produto.get("Produto", "-")))
                sku = html.escape(str(produto.get("SKU", "") or "-"))
                codigo_barras = html.escape(str(produto.get("Codigo_barras", "") or "-"))
                ncm_val = html.escape(str(produto.get("NCM", "") or "-"))
                cest_val = html.escape(str(produto.get("CEST", "") or "-"))
                categoria = html.escape(str(produto.get("Categoria", "") or "-"))
                unidade = html.escape(str(produto.get("Unidade", "") or "-"))
                status = html.escape(str(produto.get("Status", "") or "-"))
                estoque = float(produto.get("Estoque", 0) or 0)
                minimo = float(produto.get("Mínimo", 0) or 0)
                custo_unit = float(produto.get("Custo_unitário", 0) or 0)
                preco_venda = float(produto.get("Preço_venda", 0) or 0)
                margem = preco_venda - custo_unit

                is_low_stock = bool(estoque <= minimo)
                card_class = "product-card-low" if is_low_stock else ""
                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div class="product-card-box {card_class}">
                            <div class="command-item-card">
                                <div class="command-item-title">{nome}</div>
                                <div class="command-item-meta">{categoria} • SKU: {sku} • Unidade: {unidade} • Status: {status}</div>
                                <div class="command-item-row"><strong>Cód. barras:</strong> {codigo_barras} &nbsp;|&nbsp; <strong>NCM:</strong> {ncm_val} &nbsp;|&nbsp; <strong>CEST:</strong> {cest_val}</div>
                                <div class="command-item-row"><strong>Estoque:</strong> {estoque:g} &nbsp;|&nbsp; <strong>Mínimo:</strong> {minimo:g}</div>
                                <div class="command-item-row">
                                    <strong>Custo:</strong> {brl(custo_unit)} &nbsp;|&nbsp;
                                    <strong>Venda:</strong> <span class="command-item-total">{brl(preco_venda)}</span>
                                </div>
                                <div class="command-item-row"><strong>Margem unitária:</strong> {brl(margem)}</div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.markdown('<div class="product-card-actions"></div>', unsafe_allow_html=True)
                    if bool(status == "Ativo"):
                        action_edit, action_state = st.columns(2)
                        with action_edit:
                            if st.button("Alterar", key=f"edit_product_card_{produto_id}", width="stretch"):
                                st.session_state["edit_product_id"] = produto_id
                                st.rerun()
                        with action_state:
                            if st.button("Inativar", key=f"inactive_product_card_{produto_id}", width="stretch"):
                                try:
                                    execute("UPDATE products SET active = 0 WHERE id = ?", (produto_id,))
                                    log_action("produto_inativado", f"Produto inativado | ID: {produto_id} | Nome: {nome} | SKU: {sku}")
                                    st.success("Produto enviado para excluídos/inativos.")
                                    if st.session_state.get("edit_product_id") == produto_id:
                                        st.session_state.pop("edit_product_id", None)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Não foi possível excluir/inativar o produto: {e}")
                    else:
                        action_edit, action_state = st.columns(2)
                        with action_edit:
                            if st.button("Alterar", key=f"edit_product_card_{produto_id}", width="stretch"):
                                st.session_state["edit_product_id"] = produto_id
                                st.rerun()
                        with action_state:
                            if st.button("Restaurar", key=f"restore_product_card_{produto_id}", width="stretch"):
                                try:
                                    execute("UPDATE products SET active = 1 WHERE id = ?", (produto_id,))
                                    log_action("produto_restaurado", f"Produto restaurado | ID: {produto_id} | Nome: {nome} | SKU: {sku}")
                                    st.success("Produto restaurado para a lista de produtos ativos.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Não foi possível restaurar o produto: {e}")

    edit_id = st.session_state.get("edit_product_id")
    if edit_id:
        produto_df = query_df("SELECT * FROM products WHERE id = ?", (edit_id,))
        if produto_df.empty:
            st.session_state.pop("edit_product_id", None)
            st.rerun()
        produto = produto_df.iloc[0]

        @st.dialog(f"Alterar produto #{int(edit_id)}", width="large")
        def edit_product_dialog():
            st.caption("Altere as informações do produto. Pressione Enter no formulário para salvar ou use os botões abaixo.")
            with st.form(f"form_edit_product_popup_{int(edit_id)}"):
                c0, c1 = st.columns(2)
                sku = c0.text_input("SKU", value=produto["sku"] or "")
                name = c1.text_input("Nome do produto", value=produto["name"])

                c2, c3 = st.columns(2)
                category = c2.selectbox(
                    "Categoria",
                    CATEGORIAS_PADRAO,
                    index=CATEGORIAS_PADRAO.index(produto["category"]) if produto["category"] in CATEGORIAS_PADRAO else len(CATEGORIAS_PADRAO)-1,
                )
                unit = c3.text_input("Unidade", value=produto["unit"])

                f0, f1, f2 = st.columns(3)
                barcode = f0.text_input("Código de barras", value=str(produto.get("barcode", "") or ""), placeholder="Somente números")
                ncm = f1.text_input("NCM", value=str(produto.get("ncm", "") or ""), placeholder="9 dígitos", max_chars=9)
                cest = f2.text_input("Código CEST", value=str(produto.get("cest", "") or ""), placeholder="7 dígitos", max_chars=7)

                c4, c5 = st.columns(2)
                cost_unit_txt = c4.text_input("Custo unitário", value=f"{float(produto['cost_unit']):.2f}".replace(".", ","))
                sale_price_txt = c5.text_input("Preço de venda", value=f"{float(produto['sale_price']):.2f}".replace(".", ","))
                st.caption("Quantidade em estoque é alterada pelo menu Estoque. O estoque mínimo pode ser definido no cadastro inicial e alterado depois pelo menu Estoque.")

                active = st.checkbox("Produto ativo", value=bool(produto["active"]))
                b_cancel, b_save = st.columns(2)
                # Importante: o botão Salvar é criado primeiro no código para que o Enter priorize o salvamento,
                # mas visualmente continua na coluna da direita.
                with b_save:
                    salvar = st.form_submit_button("Salvar alterações", type="primary", width="stretch")
                with b_cancel:
                    cancel = st.form_submit_button("Cancelar", width="stretch")

            components.html(
                f"""
                <script>
                (function() {{
                    const productId = "{int(edit_id)}";
                    function findByText(selector, text) {{
                        const nodes = Array.from(window.parent.document.querySelectorAll(selector));
                        return nodes.find(el => (el.innerText || el.textContent || '').trim().toLowerCase() === text.toLowerCase());
                    }}
                    function attachEnterHandler() {{
                        try {{
                            const saveButton = findByText('button', 'Salvar alterações');
                            if (!saveButton) return;
                            const dialog = saveButton.closest('[role="dialog"]') || saveButton.closest('[data-testid="stDialog"]') || window.parent.document;
                            const fields = Array.from(dialog.querySelectorAll('input, textarea, [contenteditable="true"]'));
                            fields.forEach(function(field) {{
                                if (field.dataset.productEditEnterAttached === productId) return;
                                field.dataset.productEditEnterAttached = productId;
                                field.addEventListener('keydown', function(ev) {{
                                    if (ev.key === 'Enter') {{
                                        ev.preventDefault();
                                        ev.stopPropagation();
                                        try {{
                                            field.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                            field.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                            field.blur();
                                        }} catch (e) {{}}
                                        setTimeout(function() {{
                                            saveButton.click();
                                        }}, 80);
                                    }}
                                }}, true);
                            }});
                        }} catch (e) {{}}
                    }}
                    attachEnterHandler();
                    const timer = setInterval(attachEnterHandler, 250);
                    setTimeout(function() {{ clearInterval(timer); }}, 8000);
                }})();
                </script>
                """,
                height=0,
                width=0,
            )

            if salvar:
                if not name.strip():
                    st.error("Informe o nome do produto.")
                else:
                    try:
                        old_sku = produto["sku"] or "-"
                        old_name = produto["name"]
                        old_category = produto["category"]
                        old_unit = produto["unit"]
                        old_barcode = produto.get("barcode", "") or "-"
                        old_ncm = produto.get("ncm", "") or "-"
                        old_cest = produto.get("cest", "") or "-"
                        old_cost = float(produto["cost_unit"] or 0)
                        old_price = float(produto["sale_price"] or 0)
                        old_active = "Ativo" if bool(produto["active"]) else "Inativo"
                        new_cost = parse_money_input(cost_unit_txt)
                        new_price = parse_money_input(sale_price_txt)
                        new_barcode = only_digits(barcode)
                        new_ncm = only_digits(ncm, 9)
                        new_cest = only_digits(cest, 7)
                        if new_ncm and len(new_ncm) != 9:
                            st.error("O campo NCM deve conter exatamente 9 dígitos.")
                            return
                        if new_cest and len(new_cest) != 7:
                            st.error("O campo CEST deve conter exatamente 7 dígitos.")
                            return
                        new_active = "Ativo" if active else "Inativo"
                        execute(
                            """
                            UPDATE products
                            SET sku = ?, barcode = ?, ncm = ?, cest = ?, name = ?, category = ?, unit = ?, cost_unit = ?, sale_price = ?, active = ?
                            WHERE id = ?
                            """,
                            (sku.strip() or None, new_barcode or None, new_ncm or None, new_cest or None, name.strip(), category, unit.strip() or "un", new_cost, new_price, 1 if active else 0, edit_id),
                        )
                        log_action(
                            "produto_alterado",
                            f"Produto alterado | ID: {int(edit_id)} | Antes: {old_name} ({old_sku}), cód. barras {old_barcode}, NCM {old_ncm}, CEST {old_cest}, categoria {old_category}, unidade {old_unit}, custo {brl(old_cost)}, venda {brl(old_price)}, status {old_active} | Depois: {name.strip()} ({sku.strip() or '-'}), cód. barras {new_barcode or '-'}, NCM {new_ncm or '-'}, CEST {new_cest or '-'}, categoria {category}, unidade {unit.strip() or 'un'}, custo {brl(new_cost)}, venda {brl(new_price)}, status {new_active}",
                        )
                        st.session_state.pop("edit_product_id", None)
                        st.success("Produto alterado.")
                        st.rerun()
                    except DB_INTEGRITY_ERROR:
                        st.error("Já existe um produto com esse nome ou SKU.")

            if cancel:
                st.session_state.pop("edit_product_id", None)
                st.rerun()

        edit_product_dialog()

def page_stock_entries():
    st.subheader("Estoque / Compra de produtos")
    products = product_options()
    if products.empty:
        st.info("Cadastre um produto primeiro no menu Produtos.")
        return

    st.caption("Use esta tela para compras/entradas de produtos. Ao registrar uma compra, o sistema aumenta o estoque, cria a movimentação e lança a despesa de compra automaticamente.")

    product_map = {f"{(r['sku'] + ' — ') if pd.notna(r['sku']) and str(r['sku']).strip() else ''}{r['name']} — estoque: {r['stock_qty']:g}": int(r["id"]) for _, r in products.iterrows()}
    with st.form("form_stock_entry"):
        c1, c2 = st.columns(2)
        entry_date = c1.date_input("Data da compra", value=date.today(), format="DD/MM/YYYY")
        product_label = c2.selectbox("Produto", list(product_map.keys()))
        c3, c4 = st.columns(2)
        qty = c3.text_input("Quantidade comprada", placeholder="Ex: 10", key="stock_entry_qty_text")
        unit_cost = c4.text_input("Custo unitário pago", placeholder="Ex: 5,50", key="stock_entry_unit_cost_text")
        c5, c6 = st.columns(2)
        supplier = c5.text_input("Fornecedor")
        payment_method = c6.selectbox("Forma de pagamento", PAYMENT_METHODS if 'PAYMENT_METHODS' in globals() else ["Dinheiro", "Pix", "Cartão de crédito", "Cartão de débito", "Transferência", "Outro"])
        notes = st.text_area("Observações")
        submitted = st.form_submit_button("Registrar compra / entrada")
        if submitted:
            try:
                qty_value = parse_money_input(qty)
                unit_cost_value = parse_money_input(unit_cost)
                add_stock_entry(entry_date, product_map[product_label], qty_value, unit_cost_value, supplier, notes, payment_method)
                st.success("Compra registrada, estoque atualizado e despesa lançada automaticamente.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    st.divider()
    st.write("**Produtos cadastrados**")
    st.caption("Cards de estoque atual. Produtos no estoque mínimo ou abaixo ficam destacados em vermelho transparente.")

    stock_df = query_df(
        """
        SELECT id, sku AS SKU, name AS Produto, category AS Categoria, unit AS Unidade, stock_qty AS Estoque,
               min_stock AS Mínimo, cost_unit AS Custo_unitário, sale_price AS Preço_venda,
               CASE WHEN active = 1 THEN 'Ativo' ELSE 'Excluído/Inativo' END AS Status
        FROM products
        ORDER BY active DESC, category, name
        """
    )

    if stock_df.empty:
        st.info("Nenhum produto cadastrado ainda.")
    else:
        produtos_estoque = list(stock_df.iterrows())
        for row_start in range(0, len(produtos_estoque), 2):
            row_cols = st.columns(2)
            for col_index, (_, produto) in enumerate(produtos_estoque[row_start:row_start + 2]):
                with row_cols[col_index]:
                    produto_id = int(produto["id"])
                    nome = html.escape(str(produto.get("Produto", "-")))
                    sku = html.escape(str(produto.get("SKU", "") or "-"))
                    categoria = html.escape(str(produto.get("Categoria", "") or "-"))
                    unidade = html.escape(str(produto.get("Unidade", "") or "-"))
                    status = html.escape(str(produto.get("Status", "") or "-"))
                    estoque = float(produto.get("Estoque", 0) or 0)
                    minimo = float(produto.get("Mínimo", 0) or 0)
                    custo_unit = float(produto.get("Custo_unitário", 0) or 0)
                    preco_venda = float(produto.get("Preço_venda", 0) or 0)
                    margem = preco_venda - custo_unit
                    is_low_stock = bool(estoque <= minimo)
                    low_style = "background: rgba(190, 30, 30, 0.10); border-color: rgba(190, 30, 30, 0.35);" if is_low_stock else ""
                    low_msg = "<div class='command-item-row'><strong>Atenção:</strong> estoque no mínimo ou abaixo do mínimo.</div>" if is_low_stock else ""
                    with st.container(border=True):
                        st.markdown(
                            f"""
                            <div class="command-item-card" style="{low_style}">
                                <div class="command-item-title">{nome}</div>
                                <div class="command-item-meta">{categoria} • SKU: {sku} • Unidade: {unidade} • Status: {status}</div>
                                <div class="command-item-row">
                                    <strong>Estoque atual:</strong> {estoque:g} &nbsp;|&nbsp;
                                    <strong>Estoque mínimo:</strong> {minimo:g}
                                </div>
                                <div class="command-item-row">
                                    <strong>Custo:</strong> {brl(custo_unit)} &nbsp;|&nbsp;
                                    <strong>Venda:</strong> <span class="command-item-total">{brl(preco_venda)}</span>
                                </div>
                                <div class="command-item-row"><strong>Margem unitária:</strong> {brl(margem)}</div>
                                {low_msg}
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
                        _, btn_col = st.columns([2.4, 1])
                        with btn_col:
                            if st.button("Alterar mínimo", key=f"open_min_stock_popup_{produto_id}", width="stretch"):
                                st.session_state["edit_min_stock_product_id"] = produto_id
                                st.rerun()

    edit_min_id = st.session_state.get("edit_min_stock_product_id")
    if edit_min_id:
        min_prod_df = query_df("SELECT id, sku, name, category, unit, stock_qty, min_stock FROM products WHERE id = ?", (int(edit_min_id),))
        if min_prod_df.empty:
            debug_min_stock_log(f"POPUP_MIN_STOCK_PRODUCT_NOT_FOUND | product_id={edit_min_id}")
            st.session_state.pop("edit_min_stock_product_id", None)
            st.rerun()
        min_prod = min_prod_df.iloc[0]
        debug_min_stock_log(
            f"POPUP_MIN_STOCK_OPENED | product_id={int(edit_min_id)} | product={min_prod['name']} | current_min={float(min_prod['min_stock'] or 0):g}"
        )

        def _save_min_stock_from_popup(product_id: int, product_name: str, value_raw: str, trigger: str):
            debug_min_stock_log(f"MIN_STOCK_SAVE_TRIGGERED | trigger={trigger} | product_id={product_id} | raw={value_raw!r}")
            try:
                min_stock_value = parse_money_input(value_raw)
                debug_min_stock_log(f"MIN_STOCK_VALUE_PARSED | product_id={product_id} | parsed={float(min_stock_value):g}")
                if min_stock_value < 0:
                    st.session_state[f"min_stock_error_{product_id}"] = "O estoque mínimo não pode ser negativo."
                    debug_min_stock_log(f"MIN_STOCK_VALIDATION_ERROR | product_id={product_id} | reason=negative")
                    return False

                before_df = query_df("SELECT min_stock FROM products WHERE id = ?", (int(product_id),))
                old_value = float(before_df.iloc[0]["min_stock"] or 0) if not before_df.empty else None
                debug_min_stock_log(
                    f"MIN_STOCK_DB_UPDATE_ATTEMPT | product_id={product_id} | old={old_value} | new={float(min_stock_value):g}"
                )
                execute("UPDATE products SET min_stock = ? WHERE id = ?", (float(min_stock_value), int(product_id)))
                after_df = query_df("SELECT min_stock FROM products WHERE id = ?", (int(product_id),))
                saved_value = float(after_df.iloc[0]["min_stock"] or 0) if not after_df.empty else None
                debug_min_stock_log(f"MIN_STOCK_DB_UPDATE_SUCCESS | product_id={product_id} | saved={saved_value}")
                log_action("stock_min_updated", f"Estoque mínimo alterado | Produto: {product_name} | Mínimo anterior: {old_value:g} | Novo mínimo: {float(min_stock_value):g}")
                st.session_state.pop("edit_min_stock_product_id", None)
                st.session_state.pop(f"min_stock_text_{product_id}", None)
                st.session_state.pop(f"min_stock_error_{product_id}", None)
                st.session_state["min_stock_saved_message"] = "Estoque mínimo atualizado."
                return True
            except Exception as exc:
                st.session_state[f"min_stock_error_{product_id}"] = f"Erro ao salvar estoque mínimo: {exc}"
                debug_min_stock_log(f"MIN_STOCK_DB_UPDATE_ERROR | product_id={product_id} | error={repr(exc)}")
                return False

        @st.dialog(f"Configurar estoque mínimo #{int(edit_min_id)}")
        def edit_min_stock_dialog():
            st.caption("Digite o estoque mínimo e pressione Enter para salvar. O botão Salvar mínimo também funciona.")
            st.markdown(f"**Produto:** {min_prod['name']}")
            st.markdown(f"SKU: {min_prod['sku'] or '-'} • Estoque atual: **{float(min_prod['stock_qty'] or 0):g}**")

            min_key = f"min_stock_text_{int(edit_min_id)}"
            if min_key not in st.session_state:
                st.session_state[min_key] = f"{float(min_prod['min_stock'] or 0):g}"

            with st.form(f"form_min_stock_popup_{int(edit_min_id)}", clear_on_submit=False):
                min_value_raw = st.text_input("Estoque mínimo", key=min_key)
                error_key = f"min_stock_error_{int(edit_min_id)}"
                if st.session_state.get(error_key):
                    st.warning(st.session_state.get(error_key))

                b_cancel, b_save = st.columns(2)
                # Importante: o botão Salvar é criado primeiro no código para que Enter acione o salvamento,
                # mas visualmente continua na coluna da direita.
                with b_save:
                    submitted_save = st.form_submit_button("Salvar mínimo", type="primary", width="stretch")
                with b_cancel:
                    submitted_cancel = st.form_submit_button("Cancelar", width="stretch")

            components.html(
                f"""
                <script>
                (function() {{
                    const productId = "{int(edit_min_id)}";
                    function findByText(selector, text) {{
                        const nodes = Array.from(window.parent.document.querySelectorAll(selector));
                        return nodes.find(el => (el.innerText || el.textContent || '').trim().toLowerCase() === text.toLowerCase());
                    }}
                    function attachEnterHandler() {{
                        try {{
                            const labels = Array.from(window.parent.document.querySelectorAll('label, p, span, div'));
                            const label = labels.find(el => (el.innerText || '').trim() === 'Estoque mínimo');
                            let input = null;
                            if (label) {{
                                const container = label.closest('[data-testid="stTextInput"]') || label.parentElement;
                                if (container) input = container.querySelector('input');
                            }}
                            if (!input) {{
                                const inputs = Array.from(window.parent.document.querySelectorAll('input'));
                                input = inputs.find(el => (el.getAttribute('aria-label') || '').toLowerCase().includes('estoque mínimo')) || inputs[inputs.length - 1];
                            }}
                            const saveButton = findByText('button', 'Salvar mínimo');
                            if (!input || !saveButton || input.dataset.minStockEnterAttached === productId) return;
                            input.dataset.minStockEnterAttached = productId;
                            input.addEventListener('keydown', function(ev) {{
                                if (ev.key === 'Enter') {{
                                    ev.preventDefault();
                                    ev.stopPropagation();
                                    saveButton.click();
                                }}
                            }}, true);
                        }} catch (e) {{}}
                    }}
                    attachEnterHandler();
                    const timer = setInterval(attachEnterHandler, 250);
                    setTimeout(function() {{ clearInterval(timer); }}, 8000);
                }})();
                </script>
                """,
                height=0,
                width=0,
            )

            if submitted_cancel:
                debug_min_stock_log(f"MIN_STOCK_CANCEL_CLICKED | product_id={int(edit_min_id)}")
                st.session_state.pop("edit_min_stock_product_id", None)
                st.session_state.pop(min_key, None)
                st.session_state.pop(f"min_stock_error_{int(edit_min_id)}", None)
                st.rerun()

            if submitted_save:
                saved = _save_min_stock_from_popup(int(edit_min_id), str(min_prod['name']), st.session_state.get(min_key, min_value_raw), "form_submit_or_enter")
                if saved:
                    st.rerun()

        edit_min_stock_dialog()



def page_sales():
    st.subheader("Vendas / Consumo de estoque")
    products = product_options()
    if products.empty:
        st.info("Cadastre um produto primeiro.")
        return

    product_map = {f"{(r['sku'] + ' — ') if pd.notna(r['sku']) and str(r['sku']).strip() else ''}{r['name']} — {r['category']} — estoque: {r['stock_qty']:g} — preço: {brl(r['sale_price'])}": int(r["id"]) for _, r in products.iterrows()}
    events = event_options()
    event_labels = ["Sem vincular a jogo/evento"] + [f"{format_date_br(r['event_date'])} — {r['name']}" for _, r in events.iterrows()]
    event_ids = {"Sem vincular a jogo/evento": None}
    event_ids.update({f"{format_date_br(r['event_date'])} — {r['name']}": int(r["id"]) for _, r in events.iterrows()})

    comandas = command_options("Aberta")
    command_labels = ["Sem comanda"] + [f"#{int(r['number'])} — {r['customer_name'] or 'sem nome'} — {r['event_name']}" for _, r in comandas.iterrows()]
    command_ids = {"Sem comanda": None}
    command_ids.update({f"#{int(r['number'])} — {r['customer_name'] or 'sem nome'} — {r['event_name']}": int(r["id"]) for _, r in comandas.iterrows()})

    operadores = operators_options()
    operator_labels = ["Sem operador"] + [str(r["name"]) for _, r in operadores.iterrows()]
    operator_ids = {"Sem operador": None}
    operator_ids.update({str(r["name"]): int(r["id"]) for _, r in operadores.iterrows()})

    with st.form("form_sale"):
        c1, c2 = st.columns(2)
        sale_date = c1.date_input("Data da venda", value=date.today(), format="DD/MM/YYYY")
        event_label = c2.selectbox("Jogo/evento", event_labels)
        ccmd, cop = st.columns(2)
        command_label = ccmd.selectbox("Comanda (opcional)", command_labels)
        operator_label = cop.selectbox("Operador (opcional)", operator_labels)
        product_label = st.selectbox("Produto vendido/consumido", list(product_map.keys()))
        product_id = product_map[product_label]
        selected = products[products["id"] == product_id].iloc[0]
        c3, c4 = st.columns(2)
        qty = c3.number_input("Quantidade", min_value=0.01, step=1.0)
        unit_price = c4.number_input("Preço unitário de venda", min_value=0.0, value=float(selected["sale_price"]), step=0.10, format="%.2f")
        notes = st.text_area("Observações")
        submitted = st.form_submit_button("Registrar venda/baixa")
        if submitted:
            try:
                add_sale(sale_date, product_id, event_ids[event_label], qty, unit_price, notes, command_id=command_ids[command_label], operator_id=operator_ids[operator_label])
                st.success("Venda registrada e estoque baixado.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    df = query_df(
        """
        SELECT s.sale_date AS Data, COALESCE(e.name, '-') AS Jogo_evento,
               CASE WHEN c.number IS NULL THEN '-' ELSE '#' || c.number END AS Comanda,
               COALESCE(o.name, '-') AS Operador_jogador, p.sku AS SKU, p.name AS Produto, p.category AS Categoria,
               s.qty AS Quantidade, s.unit_price AS Preço_unitário, s.revenue AS Receita,
               s.cost_unit_at_sale AS Custo_unitário, s.cogs AS Custo_total,
               s.revenue - s.cogs AS Lucro_bruto, s.notes AS Observações
        FROM sales s
        JOIN products p ON p.id = s.product_id
        LEFT JOIN events e ON e.id = s.event_id
        LEFT JOIN commands c ON c.id = s.command_id
        LEFT JOIN operators o ON o.id = s.operator_id
        ORDER BY s.sale_date DESC, s.id DESC
        """
    )
    pretty_dataframe(format_date_columns(df), width="stretch", hide_index=True)


def page_events():
    st.subheader("Jogos / Eventos")
    st.info("Aqui a receita de entrada é calculada pela quantidade de jogadores com aluguel + jogadores com equipamento próprio.")
    with st.form("form_event"):
        c1, c2 = st.columns(2)
        event_date = c1.date_input("Data", value=date.today(), format="DD/MM/YYYY")
        name = c2.text_input("Nome do jogo/evento", value=f"Jogo {date.today().strftime('%d/%m/%Y')}")

        st.write("**Entradas do dia**")
        c3, c4 = st.columns(2)
        rental_qty = c3.number_input("Quantidade de aluguéis", min_value=0, step=1)
        rental_unit_price = c4.number_input("Valor unitário do aluguel", min_value=0.0, step=10.0, format="%.2f")
        c5, c6 = st.columns(2)
        own_equipment_qty = c5.number_input("Quantidade de entradas com equipamento próprio", min_value=0, step=1)
        own_equipment_unit_price = c6.number_input("Valor unitário da entrada com equipamento próprio", min_value=0.0, step=10.0, format="%.2f")

        players = int(rental_qty) + int(own_equipment_qty)
        entry_revenue = float(rental_qty) * float(rental_unit_price) + float(own_equipment_qty) * float(own_equipment_unit_price)
        c7, c8 = st.columns(2)
        c7.metric("Total de jogadores no dia", players)
        c8.metric("Total financeiro de entradas", brl(entry_revenue))

        notes = st.text_area("Observações")
        submitted = st.form_submit_button("Salvar jogo/evento")
        if submitted:
            if not name.strip():
                st.error("Informe o nome do jogo/evento.")
            else:
                execute(
                    """
                    INSERT INTO events (event_date, name, players, rental_qty, rental_unit_price, own_equipment_qty, own_equipment_unit_price, entry_revenue, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (str(event_date), name.strip(), players, int(rental_qty), float(rental_unit_price), int(own_equipment_qty), float(own_equipment_unit_price), entry_revenue, notes),
                )
                st.success("Jogo/evento cadastrado.")
                st.rerun()

    df = query_df(
        """
        SELECT e.id, e.event_date AS Data, e.name AS Evento,
               e.rental_qty AS Qtd_aluguéis, e.rental_unit_price AS Valor_unit_aluguel,
               e.own_equipment_qty AS Qtd_equipamento_próprio, e.own_equipment_unit_price AS Valor_unit_equip_próprio,
               e.players AS Total_jogadores,
               e.entry_revenue AS Total_entradas,
               COALESCE(SUM(s.revenue), 0) AS Receita_produtos,
               COALESCE(SUM(s.cogs), 0) AS Custo_produtos,
               COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id), 0) AS Despesas,
               e.entry_revenue + COALESCE(SUM(s.revenue), 0) AS Receita_total,
               e.entry_revenue + COALESCE(SUM(s.revenue), 0) - COALESCE(SUM(s.cogs), 0) - COALESCE((SELECT SUM(amount) FROM expenses ex WHERE ex.event_id = e.id), 0) AS Lucro_liquido
        FROM events e
        LEFT JOIN sales s ON s.event_id = e.id
        GROUP BY e.id
        ORDER BY e.event_date DESC, e.id DESC
        """
    )
    pretty_dataframe(format_date_columns(df), width="stretch", hide_index=True)

def page_expenses():
    st.subheader("Despesas")
    events = event_options()
    event_labels = ["Sem vincular a jogo/evento"] + [f"{format_date_br(r['event_date'])} — {r['name']}" for _, r in events.iterrows()]
    event_ids = {"Sem vincular a jogo/evento": None}
    event_ids.update({f"{format_date_br(r['event_date'])} — {r['name']}": int(r["id"]) for _, r in events.iterrows()})
    event_label_by_id = {None: "Sem vincular a jogo/evento"}
    for label, eid in event_ids.items():
        event_label_by_id[eid] = label

    with st.form("form_expense"):
        c1, c2 = st.columns(2)
        expense_date = c1.date_input("Data da despesa", value=date.today(), format="DD/MM/YYYY")
        event_label = c2.selectbox("Jogo/evento", event_labels)
        category = st.selectbox("Categoria", ["Aluguel", "Funcionário", "Manutenção", "Marketing", "Imposto", "Outro"])
        description = st.text_input("Descrição")
        amount = st.text_input("Valor", placeholder="Ex: 120,00", key="expense_page_amount")
        notes = st.text_area("Observações")
        submitted = st.form_submit_button("Salvar despesa")
        if submitted:
            amount_value = parse_money_input(amount)
            if not description.strip():
                st.error("Informe a descrição da despesa.")
            elif amount_value <= 0:
                st.error("Informe um valor maior que zero.")
            else:
                new_expense_id = execute(
                    """
                    INSERT INTO expenses (expense_date, category, description, amount, event_id, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (str(expense_date), category, description.strip(), amount_value, event_ids[event_label], notes),
                )
                log_action("despesa_criada", f"Despesa criada | ID: {new_expense_id} | Data: {format_date_br(expense_date)} | Categoria: {category} | Descrição: {description.strip()} | Valor: {brl(amount_value)} | Jogo/evento: {event_label} | Observações: {notes or '-'}")
                st.success("Despesa cadastrada.")
                st.rerun()

    st.write("**Despesas operacionais**")
    st.caption("Use esta área para despesas que não são compra de produto para revenda. Compras de produtos devem ser registradas no menu Estoque.")

    raw_df = query_df(
        """
        SELECT ex.id AS ID, ex.expense_date AS Data, ex.event_id AS event_id,
               COALESCE(e.name, '-') AS Jogo_evento, ex.category AS Categoria,
               ex.description AS Descrição, ex.amount AS Valor, ex.notes AS Observações
        FROM expenses ex
        LEFT JOIN events e ON e.id = ex.event_id
        WHERE NOT (ex.category = 'Compra de produtos' OR COALESCE(ex.notes, '') LIKE '%AUTO_COMPRA_ESTOQUE=1%')
        ORDER BY ex.expense_date DESC, ex.id DESC
        """
    )

    if raw_df.empty:
        st.info("Nenhuma despesa operacional cadastrada ainda.")
        edited_df = pd.DataFrame()
    else:
        edit_df = raw_df.copy()
        edit_df.insert(0, "Selecionar", False)
        edit_df["Jogo / evento"] = edit_df["event_id"].apply(lambda x: event_label_by_id.get(None if pd.isna(x) else int(x), "Sem vincular a jogo/evento"))
        edit_df["Data"] = pd.to_datetime(edit_df["Data"], errors="coerce").dt.date
        edit_df = edit_df[["Selecionar", "ID", "Data", "Jogo / evento", "Categoria", "Descrição", "Valor", "Observações"]]

        edited_df = st.data_editor(
        edit_df,
        width="stretch",
        hide_index=True,
        key="expenses_page_editor_v70",
        column_config={
            "Selecionar": st.column_config.CheckboxColumn("Selecionar"),
            "ID": st.column_config.NumberColumn("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
            "Jogo / evento": st.column_config.SelectboxColumn("Jogo / evento", options=event_labels),
            "Categoria": st.column_config.SelectboxColumn("Categoria", options=["Aluguel", "Funcionário", "Manutenção", "Marketing", "Imposto", "Outro"]),
            "Valor": st.column_config.NumberColumn("Valor", min_value=0.01, step=1.0, format="R$ %.2f"),
            "Observações": st.column_config.TextColumn("Observações"),
        },
    )

    col_save, col_delete = st.columns([1, 1])
    if col_save.button("Salvar alterações", type="primary"):
        try:
            for _, row in edited_df.iterrows():
                expense_id = int(row["ID"])
                expense_date_value = row["Data"]
                if pd.isna(expense_date_value):
                    raise ValueError(f"Informe uma data válida para a despesa ID {expense_id}.")
                expense_date_str = str(expense_date_value)
                if "/" in expense_date_str:
                    expense_date_str = datetime.strptime(expense_date_str, "%d/%m/%Y").date().isoformat()
                else:
                    expense_date_str = pd.to_datetime(expense_date_str).date().isoformat()

                amount_value = float(row["Valor"] or 0)
                if amount_value <= 0:
                    raise ValueError(f"O valor da despesa ID {expense_id} precisa ser maior que zero.")
                description_value = str(row["Descrição"] or "").strip()
                if not description_value:
                    raise ValueError(f"Informe a descrição da despesa ID {expense_id}.")
                event_id_value = event_ids.get(row["Jogo / evento"])
                before_expense = query_df("SELECT expense_date, category, description, amount, notes FROM expenses WHERE id = ?", (expense_id,))
                execute(
                    """
                    UPDATE expenses
                    SET expense_date = ?, event_id = ?, category = ?, description = ?, amount = ?, notes = ?
                    WHERE id = ?
                    """,
                    (
                        expense_date_str,
                        event_id_value,
                        str(row["Categoria"]),
                        description_value,
                        amount_value,
                        str(row["Observações"] or ""),
                        expense_id,
                    ),
                )
                if not before_expense.empty:
                    old_exp = before_expense.iloc[0]
                    log_action(
                        "despesa_alterada",
                        f"Despesa alterada | ID: {expense_id} | Antes: data {format_date_br(old_exp['expense_date'])}, categoria {old_exp['category']}, descrição {old_exp['description']}, valor {brl(float(old_exp['amount'] or 0))} | Depois: data {format_date_br(expense_date_str)}, categoria {str(row['Categoria'])}, descrição {description_value}, valor {brl(amount_value)}",
                    )
            st.success("Despesas atualizadas com sucesso.")
            st.rerun()
        except Exception as e:
            st.error(str(e))

    if col_delete.button("Excluir selecionadas"):
        selected = edited_df[edited_df["Selecionar"] == True]
        if selected.empty:
            st.warning("Selecione pelo menos uma despesa para excluir.")
        else:
            try:
                for _, row in selected.iterrows():
                    expense_id = int(row["ID"])
                    log_action("despesa_excluida", f"Despesa excluída | ID: {expense_id} | Descrição: {row.get('Descrição', '-')} | Valor: {brl(float(row.get('Valor', 0) or 0))}")
                    delete_expense(expense_id)
                st.success("Despesas selecionadas excluídas.")
                st.rerun()
            except Exception as e:
                st.error(str(e))

    st.divider()
    st.write("**Compras de produtos geradas pelo Estoque**")
    st.caption("Esses lançamentos são criados automaticamente quando você registra uma compra/entrada no menu Estoque. Eles não devem ser lançados manualmente em Despesas.")
    purchases_df = query_df(
        """
        SELECT ex.expense_date AS Data, ex.description AS Descrição, ex.amount AS Valor, ex.notes AS Detalhes
        FROM expenses ex
        WHERE ex.category = 'Compra de produtos' OR COALESCE(ex.notes, '') LIKE '%AUTO_COMPRA_ESTOQUE=1%'
        ORDER BY ex.expense_date DESC, ex.id DESC
        """
    )
    if purchases_df.empty:
        st.info("Nenhuma compra de produtos registrada pelo estoque ainda.")
    else:
        purchases_view = format_date_columns(purchases_df.copy())
        purchases_view["Valor"] = purchases_view["Valor"].map(brl)
        pretty_dataframe(purchases_view, width="stretch", hide_index=True)


def page_reports():
    st.subheader("Relatórios e exportações")
    start_date, end_date = render_period_filter()

    vendas = query_df(
        """
        SELECT s.sale_date AS Data, COALESCE(e.name, '-') AS Evento,
               CASE WHEN c.number IS NULL THEN '-' ELSE '#' || c.number END AS Comanda,
               COALESCE(o.name, '-') AS Operador_jogador, p.sku AS SKU, p.name AS Produto, p.category AS Categoria,
               s.qty AS Quantidade, s.unit_price AS Preço_unitário, s.revenue AS Receita,
               s.cogs AS Custo, s.revenue - s.cogs AS Lucro_bruto
        FROM sales s
        JOIN products p ON p.id = s.product_id
        LEFT JOIN events e ON e.id = s.event_id
        LEFT JOIN commands c ON c.id = s.command_id
        LEFT JOIN operators o ON o.id = s.operator_id
        WHERE DATE(s.sale_date) BETWEEN DATE(?) AND DATE(?)
        ORDER BY s.sale_date DESC
        """,
        (str(start_date), str(end_date)),
    )
    despesas = query_df(
        """
        SELECT expense_date AS Data, category AS Categoria, description AS Descrição, amount AS Valor
        FROM expenses
        WHERE DATE(expense_date) BETWEEN DATE(?) AND DATE(?)
          AND NOT (category = 'Compra de produtos' OR COALESCE(notes, '') LIKE '%AUTO_COMPRA_ESTOQUE=1%')
        ORDER BY expense_date DESC
        """,
        (str(start_date), str(end_date)),
    )
    compras_estoque = query_df(
        """
        SELECT expense_date AS Data, description AS Descrição, amount AS Valor, notes AS Detalhes
        FROM expenses
        WHERE DATE(expense_date) BETWEEN DATE(?) AND DATE(?)
          AND (category = 'Compra de produtos' OR COALESCE(notes, '') LIKE '%AUTO_COMPRA_ESTOQUE=1%')
        ORDER BY expense_date DESC
        """,
        (str(start_date), str(end_date)),
    )
    estoque = query_df(
        """
        SELECT sku AS SKU, name AS Produto, category AS Categoria, unit AS Unidade, stock_qty AS Estoque,
               min_stock AS Estoque_mínimo, cost_unit AS Custo_unitário, sale_price AS Preço_venda,
               stock_qty * cost_unit AS Valor_em_estoque
        FROM products
        ORDER BY category, name
        """
    )
    movimentos = query_df(
        """
        SELECT sm.movement_date AS Data, COALESCE(e.name, '-') AS Evento, p.sku AS SKU, p.name AS Produto,
               p.category AS Categoria, sm.movement_type AS Tipo, sm.qty AS Quantidade,
               sm.unit_cost AS Custo_unitário, sm.total_cost AS Custo_total, sm.total_revenue AS Receita, sm.notes AS Observações
        FROM stock_movements sm
        JOIN products p ON p.id = sm.product_id
        LEFT JOIN events e ON e.id = sm.event_id
        WHERE DATE(sm.movement_date) BETWEEN DATE(?) AND DATE(?)
        ORDER BY sm.movement_date DESC, sm.id DESC
        """,
        (str(start_date), str(end_date)),
    )

    st.write("**Vendas do período**")
    pretty_dataframe(format_date_columns(vendas), width="stretch", hide_index=True)
    csv_download(vendas, "vendas.csv", "Baixar vendas CSV")

    st.write("**Despesas operacionais do período**")
    pretty_dataframe(format_date_columns(despesas), width="stretch", hide_index=True)
    csv_download(despesas, "despesas_operacionais.csv", "Baixar despesas operacionais CSV")

    st.write("**Compras de estoque do período**")
    pretty_dataframe(format_date_columns(compras_estoque), width="stretch", hide_index=True)
    csv_download(compras_estoque, "compras_estoque.csv", "Baixar compras de estoque CSV")

    st.write("**Estoque atual**")
    pretty_dataframe(estoque, width="stretch", hide_index=True)
    csv_download(estoque, "estoque.csv", "Baixar estoque CSV")

    st.write("**Movimentações de estoque do período**")
    pretty_dataframe(format_date_columns(movimentos), width="stretch", hide_index=True)
    csv_download(movimentos, "movimentacoes_estoque.csv", "Baixar movimentações CSV")


def clear_all_data():
    """Limpa todas as informações sem apagar o arquivo .db.
    No Windows, apagar o arquivo do banco pode falhar porque o próprio app ainda está usando o SQLite.
    Por isso a limpeza é feita apagando os registros das tabelas e zerando os IDs.
    """
    with get_conn() as conn:
        if USE_POSTGRES:
            for table in ["cash_movements", "cash_sessions", "stock_movements", "sales", "expenses", "stock_entries", "commands", "events", "operators", "products"]:
                conn.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            conn.commit()
        else:
            conn.execute("PRAGMA foreign_keys = OFF")
            for table in ["cash_movements", "cash_sessions", "stock_movements", "sales", "expenses", "stock_entries", "commands", "events", "operators", "products"]:
                conn.execute(f"DELETE FROM {table}")
            conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('stock_movements','sales','expenses','stock_entries','commands','events','operators','products')")
            conn.commit()
            conn.execute("PRAGMA foreign_keys = ON")


def page_users():
    require_admin()
    st.subheader("Usuários do Sistema")
    st.caption("Aqui ficam os colaboradores que podem acessar o ERP. Operadores/Jogadores continuam em cadastro separado.")

    tab1, tab2, tab3 = st.tabs(["Cadastrar usuário", "Usuários cadastrados", "Logs do sistema"])

    with tab1:
        with st.form("create_system_user", clear_on_submit=True):
            c1, c2 = st.columns(2)
            name = c1.text_input("Nome *")
            username = c2.text_input("Usuário *")
            c3, c4 = st.columns(2)
            email = c3.text_input("E-mail")
            profile = c4.selectbox("Perfil de acesso *", PERFIS_USUARIO)
            c5, c6 = st.columns(2)
            password = c5.text_input("Senha *", type="password")
            password2 = c6.text_input("Confirmar senha *", type="password")
            active = st.checkbox("Usuário ativo", value=True)
            submitted = st.form_submit_button("Cadastrar usuário")

        if submitted:
            if not name.strip() or not username.strip() or not password:
                st.error("Nome, usuário e senha são obrigatórios.")
            elif password != password2:
                st.error("As senhas não conferem.")
            elif len(password) < 6:
                st.error("A senha deve ter pelo menos 6 caracteres.")
            else:
                try:
                    execute(
                        """
                        INSERT INTO system_users (name, username, email, password_hash, profile, active, must_change_password)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                        """,
                        (name.strip(), username.strip(), email.strip() or None, hash_password(password), profile, 1 if active else 0),
                    )
                    log_action("criou_usuario", f"Usuário criado: {username.strip()} / perfil {profile}")
                    st.success("Usuário cadastrado com sucesso.")
                    st.rerun()
                except DB_INTEGRITY_ERROR:
                    st.error("Já existe um usuário com esse usuário ou e-mail.")

    with tab2:
        users = query_df(
            """
            SELECT id, name AS Nome, username AS Usuário, email AS Email, profile AS Perfil,
                   CASE WHEN active = 1 THEN 'Ativo' ELSE 'Inativo' END AS Status,
                   last_login AS Ultimo_login, created_at AS Criado_em
            FROM system_users
            ORDER BY active DESC, name
            """
        )
        pretty_dataframe(users, width="stretch", hide_index=True)

        if not users.empty:
            user_map = {f"{r['Nome']} — {r['Usuário']} — {r['Perfil']}": int(r["id"]) for _, r in users.iterrows()}
            selected = st.selectbox("Selecionar usuário para alterar", ["Selecione"] + list(user_map.keys()))
            if selected != "Selecione":
                user_id = user_map[selected]
                row = query_df("SELECT * FROM system_users WHERE id = ?", (user_id,)).iloc[0]
                with st.form("edit_system_user"):
                    c1, c2 = st.columns(2)
                    edit_name = c1.text_input("Nome", value=row["name"])
                    edit_username = c2.text_input("Usuário", value=row["username"])
                    c3, c4 = st.columns(2)
                    edit_email = c3.text_input("E-mail", value=row["email"] or "")
                    edit_profile = c4.selectbox("Perfil", PERFIS_USUARIO, index=PERFIS_USUARIO.index(row["profile"]) if row["profile"] in PERFIS_USUARIO else 0)
                    edit_active = st.checkbox("Ativo", value=bool(row["active"]))
                    new_password = st.text_input("Nova senha, deixe em branco para manter", type="password")
                    save = st.form_submit_button("Salvar alterações")

                if save:
                    if not edit_name.strip() or not edit_username.strip():
                        st.error("Nome e usuário são obrigatórios.")
                    elif int(user_id) == int((current_user() or {}).get("id", 0)) and not edit_active:
                        st.error("Você não pode inativar o próprio usuário logado.")
                    else:
                        try:
                            if new_password:
                                if len(new_password) < 6:
                                    st.error("A nova senha deve ter pelo menos 6 caracteres.")
                                    return
                                execute(
                                    """
                                    UPDATE system_users
                                    SET name=?, username=?, email=?, profile=?, active=?, password_hash=?, must_change_password=0
                                    WHERE id=?
                                    """,
                                    (edit_name.strip(), edit_username.strip(), edit_email.strip() or None, edit_profile, 1 if edit_active else 0, hash_password(new_password), user_id),
                                )
                            else:
                                execute(
                                    """
                                    UPDATE system_users
                                    SET name=?, username=?, email=?, profile=?, active=?
                                    WHERE id=?
                                    """,
                                    (edit_name.strip(), edit_username.strip(), edit_email.strip() or None, edit_profile, 1 if edit_active else 0, user_id),
                                )
                            if int(user_id) == int((current_user() or {}).get("id", 0)):
                                st.session_state["auth_user"].update({"name": edit_name.strip(), "username": edit_username.strip(), "profile": edit_profile, "must_change_password": 0})
                            log_action("alterou_usuario", f"Usuário alterado: {edit_username.strip()} / perfil {edit_profile}")
                            st.success("Usuário atualizado com sucesso.")
                            st.rerun()
                        except DB_INTEGRITY_ERROR:
                            st.error("Já existe um usuário com esse usuário ou e-mail.")

    with tab3:
        logs = query_df(
            """
            SELECT l.created_at AS Data, COALESCE(u.name, 'Sistema') AS Usuario, l.action AS Acao, l.details AS Detalhes
            FROM system_logs l
            LEFT JOIN system_users u ON u.id = l.user_id
            ORDER BY l.created_at DESC
            LIMIT 300
            """
        )
        pretty_dataframe(logs, width="stretch", hide_index=True)


def page_logs():
    st.subheader("Logs / Auditoria")
    st.caption("Registro das principais ações realizadas pelos usuários no sistema.")

    users_df = query_df("SELECT id, name, username FROM system_users ORDER BY name")
    actions_df = query_df("SELECT DISTINCT action FROM system_logs ORDER BY action")

    c1, c2, c3, c4 = st.columns([1, 1, 1.2, 1.4])
    with c1:
        start_date = st.date_input("Data inicial", value=date.today().replace(day=1), format="DD/MM/YYYY", key="logs_start_date")
    with c2:
        end_date = st.date_input("Data final", value=date.today(), format="DD/MM/YYYY", key="logs_end_date")
    with c3:
        user_options = ["Todos"] + [f"{r['name']} ({r['username']})" for _, r in users_df.iterrows()]
        user_map = {f"{r['name']} ({r['username']})": int(r["id"]) for _, r in users_df.iterrows()}
        selected_user = st.selectbox("Usuário", user_options, key="logs_user_filter")
    with c4:
        action_options = ["Todas"] + (actions_df["action"].tolist() if not actions_df.empty else [])
        selected_action = st.selectbox("Ação", action_options, key="logs_action_filter")

    search_text = st.text_input("Buscar nos detalhes", placeholder="Ex: Comanda #100, PIX, nome do jogo, produto...", key="logs_search_text")

    where = ["DATE(l.created_at) BETWEEN DATE(?) AND DATE(?)"]
    params = [str(start_date), str(end_date)]
    if selected_user != "Todos":
        where.append("l.user_id = ?")
        params.append(user_map[selected_user])
    if selected_action != "Todas":
        where.append("l.action = ?")
        params.append(selected_action)
    if search_text.strip():
        where.append("COALESCE(l.details, '') LIKE ?")
        params.append(f"%{search_text.strip()}%")

    logs = query_df(
        f"""
        SELECT l.created_at AS Data,
               COALESCE(u.name, 'Sistema') AS Usuário,
               COALESCE(u.username, '-') AS Login,
               l.action AS Ação,
               l.details AS Detalhes
        FROM system_logs l
        LEFT JOIN system_users u ON u.id = l.user_id
        WHERE {' AND '.join(where)}
        ORDER BY l.created_at DESC, l.id DESC
        LIMIT 1000
        """,
        tuple(params),
    )

    if logs.empty:
        st.info("Nenhum log encontrado para os filtros selecionados.")
    else:
        pretty_dataframe(format_date_columns(logs, columns=("Data",)), width="stretch", hide_index=True)
        csv_download(logs, "logs_auditoria.csv", "Baixar logs em CSV")


def page_settings():
    st.subheader("Configurações")
    st.write("Banco de dados local:", str(DB_PATH))

    st.write("**Aparência do sistema**")
    user = current_user() or {}
    tema_atual = user.get("visual_theme") if user.get("visual_theme") in TEMAS_VISUAIS else TEMA_PADRAO
    novo_tema = st.selectbox("Tema visual", TEMAS_VISUAIS, index=TEMAS_VISUAIS.index(tema_atual), help="Escolha o visual do sistema para o seu usuário.")
    if st.button("Salvar tema visual"):
        execute("UPDATE system_users SET visual_theme = ? WHERE id = ?", (novo_tema, user.get("id")))
        st.session_state["auth_user"]["visual_theme"] = novo_tema
        st.session_state["visual_theme"] = novo_tema
        log_action("alterou_tema_visual", f"Tema visual alterado para {novo_tema}")
        st.success(f"Tema alterado para {novo_tema}.")
        st.rerun()

    st.divider()
    st.write("**Privacidade e LGPD**")
    st.caption("Modelo simples de política para uso interno do campo. Ajuste com apoio jurídico antes de uso público amplo.")
    politica_privacidade = """POLÍTICA SIMPLES DE PRIVACIDADE — TACTICAL

Coletamos apenas os dados necessários para operação do campo: nome, telefone, cidade/bairro, equipe, histórico de jogos, comandas, consumos e pagamentos.

Finalidade: identificar jogadores/operadores, controlar jogos, comandas, estoque, caixa, despesas, relatórios e auditoria operacional.

Não coletamos CPF no cadastro de jogadores/operadores.

O acesso aos dados é restrito por perfil de usuário. Exportações de dados são permitidas apenas para Administrador e Financeiro. Exclusão geral de dados é permitida apenas para Administrador.

As senhas dos usuários são armazenadas com hash seguro e não ficam salvas em texto puro.

Os dados ficam armazenados localmente no ambiente onde o sistema está instalado. O responsável pelo campo deve proteger o computador, backups e usuários de acesso.

O titular pode solicitar consulta, correção ou remoção dos dados, respeitando obrigações operacionais, financeiras e legais aplicáveis.
"""
    with st.expander("Ver termo / política simples de privacidade"):
        st.text_area("Texto da política", value=politica_privacidade, height=260)
        if can_export_data():
            st.download_button(
                "Baixar política em TXT",
                politica_privacidade.encode("utf-8"),
                file_name="politica_privacidade_tactical.txt",
                mime="text/plain",
            )

    st.divider()
    st.write("**Backup do sistema**")
    if DB_PATH.exists():
        if can_export_data():
            st.download_button(
                "Baixar backup do banco de dados",
                DB_PATH.read_bytes(),
                file_name=f"backup_mini_erp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                mime="application/octet-stream",
            )
        else:
            st.caption("Backup disponível apenas para Administrador ou Financeiro.")
    uploaded_backup = st.file_uploader("Restaurar backup .db", type=["db", "sqlite", "sqlite3"], disabled=not can_delete_system_data())
    confirmar_restore = st.text_input("Para restaurar backup, digite RESTAURAR", disabled=not can_delete_system_data())
    if not can_delete_system_data():
        st.caption("Restauração de backup disponível apenas para Administrador.")
    if st.button("Restaurar backup enviado", disabled=not can_delete_system_data()):
        if uploaded_backup is None:
            st.error("Envie um arquivo de backup primeiro.")
        elif confirmar_restore != "RESTAURAR":
            st.error("Confirmação incorreta.")
        else:
            if DB_PATH.exists():
                shutil.copy(DB_PATH, DB_PATH.with_name(f"mini_erp_backup_antes_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
            DB_PATH.write_bytes(uploaded_backup.getbuffer())
            init_db()
            st.success("Backup restaurado. A tela será atualizada.")
            st.rerun()

    st.divider()
    st.write("**Dados de exemplo**")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Inserir produtos de exemplo"):
            seed_data()
            st.success("Produtos de exemplo inseridos.")
            st.rerun()
    with col2:
        if st.button("Inserir 20 operadores de exemplo"):
            inseridos = seed_operators()
            if inseridos:
                st.success(f"{inseridos} operadores/jogadores de exemplo inseridos.")
            else:
                st.info("Os operadores/jogadores de exemplo já estavam cadastrados.")
            st.rerun()
    with col3:
        if st.button("Atualizar tela"):
            st.rerun()

    st.warning("Cuidado: o botão abaixo apaga todos os dados do sistema.")
    confirm = st.text_input("Digite APAGAR para liberar a limpeza total", disabled=not can_delete_system_data())
    if not can_delete_system_data():
        st.caption("Limpeza total disponível apenas para Administrador.")
    if st.button("Apagar todas as informações", disabled=not can_delete_system_data()):
        if confirm == "APAGAR":
            if DB_PATH.exists():
                shutil.copy(DB_PATH, DB_PATH.with_name(f"mini_erp_backup_antes_limpeza_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
            clear_all_data()
            init_db()
            st.success("Todas as informações foram apagadas. Um backup de segurança foi salvo na pasta do sistema.")
            st.rerun()
        else:
            st.error("Confirmação incorreta.")


def main():
    init_db()
    render_header()

    if not current_user():
        login_screen()
        return

    page = render_sidebar()
    if page == "Dashboard":
        page_dashboard()
    elif page == "Jogos":
        page_events_operation()
    elif page == "Operadores":
        page_operators()
    elif page == "Comandas":
        page_commands()
    elif page == "Caixa":
        page_cash()
    elif page == "Produtos":
        page_products()
    elif page == "Estoque":
        page_stock()
    elif page == "Despesas":
        page_expenses()
    elif page == "Relatórios":
        page_reports()
    elif page == "Usuários":
        page_users()
    elif page == "Logs/Auditoria":
        page_logs()
    elif page == "Configurações":
        page_settings()


if __name__ == "__main__":
    main()
