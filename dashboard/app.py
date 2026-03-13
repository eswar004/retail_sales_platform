import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# Page config
st.set_page_config(
    page_title="Retail Sales Analytics Platform",
    page_icon="🛒",
    layout="wide"
)

# Snowflake connection
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        schema='GOLD'
    )

# Query runner
@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────
st.title("🛒 Retail Sales Analytics Platform")
st.markdown("**End-to-end pipeline built with Airflow + DBT + Snowflake | Olist Brazilian E-Commerce Dataset**")
st.divider()

# ─────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────
kpi_query = """
SELECT
    ROUND(SUM(TOTAL_REVENUE), 2)                        AS total_revenue,
    COUNT(DISTINCT ORDER_ID)                            AS total_orders,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID)                  AS total_customers,
    ROUND(AVG(DELIVERY_TIME_DAYS), 1)                   AS avg_delivery_days,
    ROUND(SUM(CASE WHEN IS_LATE = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS late_pct,
    ROUND(AVG(REVIEW_SCORE), 2)                         AS avg_review_score
FROM RETAIL_DB.GOLD.FACT_ORDERS
WHERE ORDER_STATUS = 'delivered'
"""

kpis = run_query(kpi_query)

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("💰 Total Revenue", f"R$ {kpis['TOTAL_REVENUE'][0]:,.0f}")
with col2:
    st.metric("📦 Total Orders", f"{kpis['TOTAL_ORDERS'][0]:,}")
with col3:
    st.metric("👥 Unique Customers", f"{kpis['TOTAL_CUSTOMERS'][0]:,}")
with col4:
    st.metric("🚚 Avg Delivery Days", f"{kpis['AVG_DELIVERY_DAYS'][0]} days")
with col5:
    st.metric("⚠️ Late Delivery Rate", f"{kpis['LATE_PCT'][0]}%")
with col6:
    st.metric("⭐ Avg Review Score", f"{kpis['AVG_REVIEW_SCORE'][0]} / 5")

st.divider()

# ─────────────────────────────────────────
# ROW 1 — Revenue Trend + Order Status
# ─────────────────────────────────────────
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📈 Monthly Revenue Trend")
    revenue_query = """
    SELECT
        DATE_TRUNC('month', ORDER_PURCHASE_TIMESTAMP) AS month,
        ROUND(SUM(TOTAL_REVENUE), 2)                  AS revenue,
        COUNT(DISTINCT ORDER_ID)                       AS orders
    FROM RETAIL_DB.GOLD.FACT_ORDERS
    WHERE ORDER_STATUS = 'delivered'
    GROUP BY 1
    ORDER BY 1
    """
    revenue_df = run_query(revenue_query)
    fig = px.line(
        revenue_df,
        x='MONTH',
        y='REVENUE',
        markers=True,
        labels={'MONTH': 'Month', 'REVENUE': 'Revenue (R$)'},
        color_discrete_sequence=['#2196F3']
    )
    fig.update_layout(hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("🔄 Order Status")
    status_query = """
    SELECT
        ORDER_STATUS,
        COUNT(*) AS total
    FROM RETAIL_DB.GOLD.FACT_ORDERS
    GROUP BY ORDER_STATUS
    ORDER BY total DESC
    """
    status_df = run_query(status_query)
    fig = px.pie(
        status_df,
        names='ORDER_STATUS',
        values='TOTAL',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─────────────────────────────────────────
# ROW 2 — Top Categories + Delivery by State
# ─────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("🏆 Top 10 Product Categories by Revenue")
    category_query = """
    SELECT
        p.PRODUCT_CATEGORY_NAME,
        ROUND(SUM(f.TOTAL_REVENUE), 2) AS revenue
    FROM RETAIL_DB.GOLD.FACT_ORDERS f
    JOIN RETAIL_DB.GOLD.DIM_PRODUCTS p ON f.ORDER_ID = f.ORDER_ID
    WHERE f.ORDER_STATUS = 'delivered'
    GROUP BY 1
    ORDER BY revenue DESC
    LIMIT 10
    """
    cat_df = run_query(category_query)
    fig = px.bar(
        cat_df,
        x='REVENUE',
        y='PRODUCT_CATEGORY_NAME',
        orientation='h',
        labels={'REVENUE': 'Revenue (R$)', 'PRODUCT_CATEGORY_NAME': 'Category'},
        color='REVENUE',
        color_continuous_scale='Blues'
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("🚚 Avg Delivery Time by State")
    delivery_query = """
    SELECT
        c.CUSTOMER_STATE,
        ROUND(AVG(f.DELIVERY_TIME_DAYS), 1) AS avg_days
    FROM RETAIL_DB.GOLD.FACT_ORDERS f
    JOIN RETAIL_DB.GOLD.DIM_CUSTOMERS c
        ON f.CUSTOMER_UNIQUE_ID = c.CUSTOMER_UNIQUE_ID
    WHERE f.ORDER_STATUS = 'delivered'
    GROUP BY 1
    ORDER BY avg_days DESC
    LIMIT 15
    """
    delivery_df = run_query(delivery_query)
    fig = px.bar(
        delivery_df,
        x='AVG_DAYS',
        y='CUSTOMER_STATE',
        orientation='h',
        labels={'AVG_DAYS': 'Avg Days', 'CUSTOMER_STATE': 'State'},
        color='AVG_DAYS',
        color_continuous_scale='Reds'
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─────────────────────────────────────────
# ROW 3 — Sentiment + Payment Types
# ─────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("😊 Customer Sentiment")
    sentiment_query = """
    SELECT
        SENTIMENT,
        COUNT(*) AS total
    FROM RETAIL_DB.GOLD.FACT_ORDERS
    WHERE SENTIMENT IS NOT NULL
    GROUP BY SENTIMENT
    ORDER BY total DESC
    """
    sentiment_df = run_query(sentiment_query)
    color_map = {
        'positive': '#4CAF50',
        'neutral':  '#FFC107',
        'negative': '#F44336'
    }
    fig = px.pie(
        sentiment_df,
        names='SENTIMENT',
        values='TOTAL',
        hole=0.4,
        color='SENTIMENT',
        color_discrete_map=color_map
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("💳 Payment Type Distribution")
    payment_query = """
    SELECT
        PAYMENT_TYPE_CLEAN,
        COUNT(*) AS total,
        ROUND(SUM(TOTAL_PAYMENT_VALUE), 2) AS total_value
    FROM RETAIL_DB.GOLD.FACT_ORDERS
    WHERE PAYMENT_TYPE_CLEAN IS NOT NULL
    GROUP BY 1
    ORDER BY total DESC
    """
    payment_df = run_query(payment_query)
    fig = px.bar(
        payment_df,
        x='PAYMENT_TYPE_CLEAN',
        y='TOTAL',
        labels={'PAYMENT_TYPE_CLEAN': 'Payment Type', 'TOTAL': 'Number of Orders'},
        color='PAYMENT_TYPE_CLEAN',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────
st.markdown("""
**🔧 Tech Stack:** Apache Airflow • dbt Core • Snowflake • Python • Streamlit • Plotly

**📊 Dataset:** Olist Brazilian E-Commerce (Kaggle) — 99,441 orders • 2016-2018

**🏗️ Architecture:** CSV → Snowflake Staging → Bronze → Silver → Gold → Dashboard
""")