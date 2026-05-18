"""
dashboard_reporting.py  —  MedPulse Analytics Dashboard
Reads pre-computed gold layer JSON files and renders an interactive Streamlit dashboard.
 
Run with:
    streamlit run dashboard_reporting.py
"""
 
import glob
import json
import logging
import os
 
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
 
# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
 
# ─────────────────────────────────────────────────────────────
#  PAGE CONFIG  (must be the very first Streamlit call)
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MedPulse Analytics",
    page_icon="🫀",
    layout="wide",
    initial_sidebar_state="collapsed",
)
 
# ─────────────────────────────────────────────────────────────
#  CONFIG  (FIX: no more hard-coded Windows path — use env variable)
# ─────────────────────────────────────────────────────────────
GOLD_PATH = os.getenv("GOLD_OUTPUT_PATH", "/tmp/kafka_project/gold_data")
 
# ─────────────────────────────────────────────────────────────
#  THEME
# ─────────────────────────────────────────────────────────────
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True
 
DARK = {
    "bg":       "#0B0F1A",
    "surface":  "#111827",
    "card":     "#1A2235",
    "border":   "#1E3A5F",
    "text":     "#E8F4FD",
    "subtext":  "#7FB3D3",
    "accent1":  "#00D4FF",
    "accent2":  "#7C3AED",
    "accent3":  "#10B981",
    "accent4":  "#F59E0B",
    "danger":   "#EF4444",
    "chart_bg": "rgba(17,24,39,0)",
    "grid":     "rgba(30,58,95,0.4)",
}
 
LIGHT = {
    "bg":       "#F0F7FF",
    "surface":  "#FFFFFF",
    "card":     "#FFFFFF",
    "border":   "#BAD4EE",
    "text":     "#0D2B4B",
    "subtext":  "#4A7FA5",
    "accent1":  "#0284C7",
    "accent2":  "#7C3AED",
    "accent3":  "#059669",
    "accent4":  "#D97706",
    "danger":   "#DC2626",
    "chart_bg": "rgba(255,255,255,0)",
    "grid":     "rgba(186,212,238,0.6)",
}
 
T = DARK if st.session_state.dark_mode else LIGHT
 
 
# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"
 
 
NO_COLOR    = "rgba(0,0,0,0)"
_hero_bg    = f"linear-gradient(135deg,{rgba(T['accent2'],0.13)} 0%,{rgba(T['accent1'],0.08)} 50%,{rgba(T['accent3'],0.07)} 100%)"
_hero_glow  = f"radial-gradient({rgba(T['accent1'],0.13)}, transparent 70%)"
_kpi_shadow = rgba(T["accent1"], 0.13)
_kpi_delta  = rgba(T["accent3"], 0.13)
_chart_glow = rgba(T["accent1"], 0.10)
_badge_bg   = rgba(T["accent1"], 0.13)
 
 
# ─────────────────────────────────────────────────────────────
#  CSS
# ─────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap');
*,*::before,*::after{{box-sizing:border-box}}
html,body,[data-testid="stAppViewContainer"],[data-testid="stMain"]{{
    background:{T['bg']} !important;color:{T['text']} !important;
    font-family:'Syne',sans-serif !important;
}}
[data-testid="stHeader"]{{background:transparent !important}}
[data-testid="stToolbar"]{{display:none !important}}
[data-testid="stSidebar"]{{background:{T['surface']} !important}}
.block-container{{padding:1.5rem 2.5rem 3rem !important;max-width:1600px !important}}
.hero{{
    background:{_hero_bg};border:1px solid {T['border']};border-radius:20px;
    padding:2rem 2.5rem;margin-bottom:2rem;position:relative;overflow:hidden;
    animation:fadeSlideDown .6s ease both;
}}
.hero::before{{
    content:'';position:absolute;top:-60px;right:-60px;width:220px;height:220px;
    background:{_hero_glow};border-radius:50%;
}}
.hero-title{{
    font-size:2.1rem;font-weight:800;letter-spacing:-.5px;
    background:linear-gradient(90deg,{T['accent1']},{T['accent2']});
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0;
}}
.hero-sub{{color:{T['subtext']};font-size:.95rem;margin-top:.3rem;font-family:'Space Mono',monospace}}
.kpi-card{{
    background:{T['card']};border:1px solid {T['border']};border-radius:16px;
    padding:1.4rem 1.6rem;text-align:center;animation:fadeUp .5s ease both;
    transition:transform .25s ease,box-shadow .25s ease;cursor:default;
}}
.kpi-card:hover{{transform:translateY(-4px);box-shadow:0 12px 40px {_kpi_shadow}}}
.kpi-icon{{font-size:1.8rem;margin-bottom:.4rem}}
.kpi-value{{font-size:2rem;font-weight:800;color:{T['accent1']};line-height:1}}
.kpi-label{{font-size:.78rem;text-transform:uppercase;letter-spacing:1.5px;color:{T['subtext']};margin-top:.35rem;font-family:'Space Mono',monospace}}
.kpi-delta{{display:inline-block;margin-top:.4rem;font-size:.8rem;font-weight:600;padding:2px 10px;border-radius:20px;background:{_kpi_delta};color:{T['accent3']}}}
.section-header{{
    font-size:1rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;
    color:{T['subtext']};font-family:'Space Mono',monospace;
    margin:1.5rem 0 .8rem;display:flex;align-items:center;gap:.5rem;
}}
.section-header::after{{content:'';flex:1;height:1px;background:linear-gradient({T['border']},transparent)}}
.chart-card{{
    background:{T['card']};border:1px solid {T['border']};border-radius:16px;
    padding:1.2rem;animation:fadeUp .7s ease both;transition:box-shadow .3s ease;margin-bottom:.5rem;
}}
.chart-card:hover{{box-shadow:0 8px 32px {_chart_glow}}}
.chart-title{{font-size:.85rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:{T['subtext']};margin-bottom:.5rem;font-family:'Space Mono',monospace}}
.arch-footer{{
    background:linear-gradient(135deg,{T['card']},{T['surface']});
    border:1px solid {T['border']};border-radius:16px;padding:1.5rem 2rem;margin-top:2rem;
}}
.arch-item{{display:flex;align-items:flex-start;gap:.6rem;margin-bottom:.5rem;font-size:.88rem;color:{T['subtext']}}}
.arch-badge{{background:{_badge_bg};color:{T['accent1']};padding:1px 8px;border-radius:8px;font-size:.72rem;font-family:'Space Mono',monospace;font-weight:700;white-space:nowrap;margin-top:2px}}
@keyframes fadeSlideDown{{from{{opacity:0;transform:translateY(-16px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(20px)}}to{{opacity:1;transform:translateY(0)}}}}
[data-testid="stMetric"]{{display:none}}
div[data-testid="column"]>div{{gap:0 !important}}
.stDataFrame{{border-radius:12px;overflow:hidden}}
[data-testid="stDataFrame"]{{border:1px solid {T['border']} !important;border-radius:12px}}
::-webkit-scrollbar{{width:6px;height:6px}}
::-webkit-scrollbar-track{{background:{T['bg']}}}
::-webkit-scrollbar-thumb{{background:{T['border']};border-radius:3px}}
</style>
""", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  PLOTLY THEME HELPER
# ─────────────────────────────────────────────────────────────
def chart_layout(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color=T["subtext"], family="Space Mono")),
        paper_bgcolor=T["chart_bg"],
        plot_bgcolor=T["chart_bg"],
        font=dict(color=T["text"], family="Syne"),
        margin=dict(l=10, r=10, t=40 if title else 10, b=10),
        legend=dict(bgcolor=NO_COLOR, font=dict(color=T["subtext"], size=11)),
        xaxis=dict(
            gridcolor=T["grid"],
            tickfont=dict(color=T["subtext"], size=11),
            title_font=dict(color=T["subtext"]),
            showline=False, zeroline=False,
        ),
        yaxis=dict(
            gridcolor=T["grid"],
            tickfont=dict(color=T["subtext"], size=11),
            title_font=dict(color=T["subtext"]),
            showline=False, zeroline=False,
        ),
        hoverlabel=dict(
            bgcolor=T["card"], bordercolor=T["border"],
            font=dict(color=T["text"], family="Space Mono", size=12),
        ),
        transition_duration=500,
    )
    return fig
 
 
# ─────────────────────────────────────────────────────────────
#  DATA LOADING  (cached for 5 minutes so the dashboard is fast)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_gold_data() -> dict:
    """
    Reads all gold JSON files from disk into Python dicts.
    ttl=300 means Streamlit refreshes this every 5 minutes automatically.
    """
    def load_jsonl(pattern: str):
        files = glob.glob(pattern)
        if not files:
            return None
        rows = []
        for filepath in files:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return rows if rows else None
 
    metrics = {
        "age_group": load_jsonl(f"{GOLD_PATH}/age_group_summary/part-*.json"),
        "gender":    load_jsonl(f"{GOLD_PATH}/gender_summary/part-*.json"),
        "wbc":       load_jsonl(f"{GOLD_PATH}/wbc_summary/part-*.json"),
        "hgb":       load_jsonl(f"{GOLD_PATH}/hgb_summary/part-*.json"),
        "severity":  load_jsonl(f"{GOLD_PATH}/severity_summary/part-*.json"),
        "diagnosis": load_jsonl(f"{GOLD_PATH}/diagnosis_summary/part-*.json"),
        "doctors":   load_jsonl(f"{GOLD_PATH}/top_doctors/part-*.json"),
    }
    # Remove keys where no data was found
    return {k: v for k, v in metrics.items() if v is not None}
 
 
def demo_data() -> dict:
    """
    Fallback data shown when gold layer hasn't been generated yet.
    Lets you demo the dashboard without running the full pipeline.
    FIX: gender values now match the silver layer (MALE/FEMALE uppercase).
    """
    return {
        "age_group": [
            {"age_group": "YOUNG_ADULT", "patient_count": 312, "avg_severity_score": 1.4},
            {"age_group": "MIDDLE_AGED", "patient_count": 487, "avg_severity_score": 2.1},
            {"age_group": "SENIOR",      "patient_count": 634, "avg_severity_score": 2.8},
            {"age_group": "ELDERLY",     "patient_count": 291, "avg_severity_score": 3.5},
        ],
        # FIX: was "Male"/"Female" — now matches consumer.py output ("MALE"/"FEMALE")
        "gender": [
            {"gender": "MALE",   "patient_count": 842},
            {"gender": "FEMALE", "patient_count": 882},
        ],
        "wbc": [
            {"wbc_status": "NORMAL", "patient_count": 910},
            {"wbc_status": "HIGH",   "patient_count": 420},
            {"wbc_status": "LOW",    "patient_count": 394},
        ],
        "hgb": [
            {"hgb_status": "NORMAL", "patient_count": 850},
            {"hgb_status": "LOW",    "patient_count": 530},
            {"hgb_status": "HIGH",   "patient_count": 344},
        ],
        "severity": [
            {"severity": "MILD",     "patient_count": 720, "avg_age": 38.0},
            {"severity": "MODERATE", "patient_count": 580, "avg_age": 52.0},
            {"severity": "SEVERE",   "patient_count": 424, "avg_age": 67.0},
        ],
        "diagnosis": [
            {"diagnosis_code": "I10",   "diagnosis_desc": "Essential hypertension",          "patient_count": 350},
            {"diagnosis_code": "E11.9", "diagnosis_desc": "Type 2 diabetes",                 "patient_count": 290},
            {"diagnosis_code": "J45.9", "diagnosis_desc": "Asthma, unspecified",             "patient_count": 210},
            {"diagnosis_code": "J18.9", "diagnosis_desc": "Pneumonia",                       "patient_count": 180},
            {"diagnosis_code": "I21.9", "diagnosis_desc": "Acute myocardial infarction",     "patient_count": 145},
            {"diagnosis_code": "N18.3", "diagnosis_desc": "Chronic kidney disease stage 3",  "patient_count": 130},
            {"diagnosis_code": "F32.9", "diagnosis_desc": "Major depressive disorder",       "patient_count": 118},
            {"diagnosis_code": "M54.5", "diagnosis_desc": "Low back pain",                   "patient_count": 105},
        ],
        "doctors": [
            {"doctor_id": 1234, "doctor_name": "Dr. Patel",  "patients_treated": 87},
            {"doctor_id": 5678, "doctor_name": "Dr. Khan",   "patients_treated": 74},
            {"doctor_id": 9101, "doctor_name": "Dr. Sharma", "patients_treated": 68},
        ],
    }
 
 
# ─────────────────────────────────────────────────────────────
#  LOAD DATA
# ─────────────────────────────────────────────────────────────
gold_data  = load_gold_data()
using_demo = not bool(gold_data)
if using_demo:
    gold_data = demo_data()
    log.warning("No gold data found — using demo data.")
 
# ─────────────────────────────────────────────────────────────
#  COMPUTE KPI METRICS
# ─────────────────────────────────────────────────────────────
total_patients = severe_count = avg_age = male_pct = 0
 
if "age_group" in gold_data:
    ag             = gold_data["age_group"]
    total_patients = sum(i.get("patient_count", 0) for i in ag)
    severe_count   = sum(i.get("patient_count", 0) for i in ag if i.get("avg_severity_score", 0) > 2)
    age_map        = {"YOUNG_ADULT": 25, "MIDDLE_AGED": 45, "SENIOR": 65, "ELDERLY": 80}
    total_age_sum  = sum(age_map.get(i.get("age_group", ""), 0) * i.get("patient_count", 0) for i in ag)
    avg_age        = round(total_age_sum / total_patients, 1) if total_patients else 0
 
severe_pct  = round(severe_count / total_patients * 100, 1) if total_patients else 0
 
if "gender" in gold_data:
    gd      = gold_data["gender"]
    # FIX: now correctly matches MALE/FEMALE (uppercase) from the real pipeline
    male    = next((i["patient_count"] for i in gd if i.get("gender", "").upper() == "MALE"),   0)
    female  = next((i["patient_count"] for i in gd if i.get("gender", "").upper() == "FEMALE"), 0)
    total_g = male + female
    male_pct = round(male / total_g * 100, 1) if total_g else 0
 
# ─────────────────────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────────────────────
top_l, top_r = st.columns([5, 1])
with top_l:
    st.markdown(f"""
    <div class="hero">
        <div class="hero-title">🫀 MedPulse Analytics</div>
        <div class="hero-sub">Gold Layer Intelligence · Real-time Patient Metrics · {'🟡 Demo Mode' if using_demo else '🟢 Live Data'}</div>
    </div>
    """, unsafe_allow_html=True)
 
with top_r:
    st.write(""); st.write("")
    btn_label = "☀️ Light Mode" if st.session_state.dark_mode else "🌙 Dark Mode"
    if st.button(btn_label, key="theme_toggle"):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()
 
 
# ─────────────────────────────────────────────────────────────
#  KPI CARDS
# ─────────────────────────────────────────────────────────────
SEVERITY_CLR = [T["accent3"], T["accent4"], T["danger"], "#B91C1C"]
 
st.markdown('<div class="section-header">📊 Key Performance Indicators</div>', unsafe_allow_html=True)
k1, k2, k3, k4 = st.columns(4)
 
for card_col, icon, val, lbl, delta, color in [
    (k1, "🏥", f"{total_patients:,}", "Total Patients",  None,               T["accent1"]),
    (k2, "⚠️",  str(severe_count),    "Severe Cases",    f"↑ {severe_pct}%", T["danger"]),
    (k3, "🎂",  str(avg_age),         "Average Age",     "years",            T["accent4"]),
    (k4, "⚧",  f"{male_pct}%",       "Male Patients",   "gender split",     T["accent2"]),
]:
    d = f'<div class="kpi-delta">{delta}</div>' if delta else ""
    with card_col:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-icon">{icon}</div>
            <div class="kpi-value" style="color:{color}">{val}</div>
            <div class="kpi-label">{lbl}</div>{d}
        </div>""", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  CHARTS ROW 1 — Patient Distribution
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📈 Patient Distribution</div>', unsafe_allow_html=True)
c1, c2 = st.columns(2)
 
with c1:
    st.markdown('<div class="chart-card"><div class="chart-title">👥 Age Group Distribution</div>', unsafe_allow_html=True)
    if "age_group" in gold_data:
        age_df = pd.DataFrame(gold_data["age_group"])
        fig = go.Figure(go.Bar(
            x=age_df["age_group"], y=age_df["patient_count"],
            marker=dict(
                color=age_df["patient_count"],
                colorscale=[[0, T["accent2"]], [0.5, T["accent1"]], [1, T["accent3"]]],
                line=dict(color=T["border"], width=1),
                opacity=0.92,
            ),
            hovertemplate="<b>%{x}</b><br>Patients: %{y:,}<extra></extra>",
        ))
        st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
with c2:
    st.markdown('<div class="chart-card"><div class="chart-title">🔴 Severity Score by Age Group</div>', unsafe_allow_html=True)
    if "age_group" in gold_data:
        age_df = pd.DataFrame(gold_data["age_group"])
        if "avg_severity_score" in age_df.columns:
            fig = go.Figure()
            for i, row in age_df.iterrows():
                clr = SEVERITY_CLR[min(i, len(SEVERITY_CLR) - 1)]
                fig.add_trace(go.Bar(
                    x=[row["age_group"]], y=[row["avg_severity_score"]], name=row["age_group"],
                    marker=dict(color=clr, opacity=0.9, line=dict(color=T["border"], width=1)),
                    hovertemplate=f"<b>{row['age_group']}</b><br>Avg Severity: %{{y:.2f}}<extra></extra>",
                ))
            fig.add_hline(y=2, line_dash="dot", line_color=T["danger"], opacity=0.5,
                          annotation_text="Severe threshold",
                          annotation_font_color=T["danger"], annotation_font_size=11)
            fig = chart_layout(fig)
            fig.update_layout(showlegend=False, barmode="group")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  CHARTS ROW 2 — Lab Values & Gender
# ─────────────────────────────────────────────────────────────
c3, c4 = st.columns(2)
 
with c3:
    st.markdown('<div class="chart-card"><div class="chart-title">⚧ Gender Distribution</div>', unsafe_allow_html=True)
    if "gender" in gold_data:
        gender_df = pd.DataFrame(gold_data["gender"])
        fig = go.Figure(go.Pie(
            labels=gender_df["gender"], values=gender_df["patient_count"], hole=0.55,
            marker=dict(colors=[T["accent1"], T["accent2"]], line=dict(color=T["bg"], width=3)),
            textfont=dict(color=T["text"], family="Space Mono", size=12),
            hovertemplate="<b>%{label}</b><br>%{value:,} patients<br>%{percent}<extra></extra>",
            rotation=90,
        ))
        fig.add_annotation(
            text=f"{total_patients:,}<br><span style='font-size:11px'>Patients</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=18, color=T["text"], family="Syne"), align="center",
        )
        st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
with c4:
    st.markdown('<div class="chart-card"><div class="chart-title">🩸 WBC Status</div>', unsafe_allow_html=True)
    if "wbc" in gold_data:
        wbc_df = pd.DataFrame(gold_data["wbc"])
        if not wbc_df.empty:
            # FIX: wbc_status is now uppercase (NORMAL/HIGH/LOW) from consumer.py
            wbc_colors = {"NORMAL": T["accent3"], "HIGH": T["danger"], "LOW": T["accent4"]}
            fig = go.Figure(go.Bar(
                x=wbc_df["wbc_status"], y=wbc_df["patient_count"],
                marker=dict(
                    color=[wbc_colors.get(s, T["accent1"]) for s in wbc_df["wbc_status"]],
                    opacity=0.9, line=dict(color=T["border"], width=1),
                ),
                hovertemplate="<b>WBC %{x}</b><br>Count: %{y:,}<extra></extra>",
                width=0.5,
            ))
            st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  CHARTS ROW 3 — Severity & Diagnosis (NEW)
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">🏥 Clinical Insights</div>', unsafe_allow_html=True)
c5, c6 = st.columns(2)
 
with c5:
    st.markdown('<div class="chart-card"><div class="chart-title">⚡ Severity Breakdown</div>', unsafe_allow_html=True)
    if "severity" in gold_data:
        sev_df = pd.DataFrame(gold_data["severity"])
        sev_colors = {"MILD": T["accent3"], "MODERATE": T["accent4"], "SEVERE": T["danger"], "UNKNOWN": T["subtext"]}
        fig = go.Figure(go.Bar(
            x=sev_df["severity"], y=sev_df["patient_count"],
            marker=dict(
                color=[sev_colors.get(s, T["accent1"]) for s in sev_df["severity"]],
                opacity=0.9, line=dict(color=T["border"], width=1),
            ),
            hovertemplate="<b>%{x}</b><br>Patients: %{y:,}<extra></extra>",
        ))
        st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
with c6:
    st.markdown('<div class="chart-card"><div class="chart-title">🔬 Top Diagnoses</div>', unsafe_allow_html=True)
    if "diagnosis" in gold_data:
        diag_df = pd.DataFrame(gold_data["diagnosis"])
        # Use short codes on the x-axis for readability
        fig = go.Figure(go.Bar(
            x=diag_df["diagnosis_code"], y=diag_df["patient_count"],
            marker=dict(
                color=diag_df["patient_count"],
                colorscale=[[0, T["accent2"]], [1, T["accent1"]]],
                opacity=0.9, line=dict(color=T["border"], width=1),
            ),
            customdata=diag_df["diagnosis_desc"],
            hovertemplate="<b>%{x}</b><br>%{customdata}<br>Patients: %{y:,}<extra></extra>",
        ))
        st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  COMBO CHART — Severity Risk Radar
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📉 Severity Risk Radar</div>', unsafe_allow_html=True)
 
if "age_group" in gold_data:
    age_df = pd.DataFrame(gold_data["age_group"])
    if "avg_severity_score" in age_df.columns:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=age_df["age_group"], y=age_df["avg_severity_score"],
            mode="lines+markers", name="Avg Severity",
            line=dict(color=T["accent1"], width=3, shape="spline"),
            marker=dict(size=10, color=T["accent1"], line=dict(color=T["bg"], width=2)),
            fill="tozeroy", fillcolor=rgba(T["accent1"], 0.09),
            hovertemplate="<b>%{x}</b><br>Severity: %{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            x=age_df["age_group"], y=age_df["patient_count"],
            name="Patient Count",
            marker=dict(color=rgba(T["accent2"], 0.25)),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Patients: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            yaxis2=dict(
                overlaying="y", side="right",
                tickfont=dict(color=T["subtext"], size=10),
                gridcolor=NO_COLOR, zeroline=False, showline=False,
                title=dict(text="Patient Count", font=dict(color=T["subtext"], size=11)),
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )
        st.plotly_chart(chart_layout(fig), use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)
 
 
# ─────────────────────────────────────────────────────────────
#  TOP DOCTORS TABLE (NEW)
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">👨‍⚕️ Top Doctors by Patient Load</div>', unsafe_allow_html=True)
if "doctors" in gold_data:
    doc_df = pd.DataFrame(gold_data["doctors"])
    doc_df.columns = [c.replace("_", " ").title() for c in doc_df.columns]
    st.dataframe(doc_df, use_container_width=True, hide_index=True)
 
 
# ─────────────────────────────────────────────────────────────
#  RAW DATA EXPANDER
# ─────────────────────────────────────────────────────────────
with st.expander("📋 Raw Gold Layer Data", expanded=False):
    if "age_group" in gold_data:
        df = pd.DataFrame(gold_data["age_group"])
        df.columns = [c.replace("_", " ").title() for c in df.columns]
        numeric_cols = [c for c in df.columns if "Count" in c or "Score" in c]
        st.dataframe(
            df.style.background_gradient(cmap="Blues", subset=numeric_cols),
            use_container_width=True, height=220,
        )
 
 
# ─────────────────────────────────────────────────────────────
#  FOOTER
# ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="arch-footer">
    <div style="font-size:1rem;font-weight:700;margin-bottom:.8rem;color:{T['text']}">🏗️ Data Pipeline Architecture</div>
    <div class="arch-item"><span class="arch-badge">BRONZE</span> Raw Kafka messages → JSON on disk (unmodified)</div>
    <div class="arch-item"><span class="arch-badge">SILVER</span> Cleaned, validated, enriched records (PySpark Structured Streaming)</div>
    <div class="arch-item"><span class="arch-badge">GOLD</span>   Pre-aggregated summaries for instant dashboard queries</div>
    <div class="arch-item"><span class="arch-badge">LIVE</span>   Auto-refreshes every 5 minutes via Streamlit cache</div>
</div>
<div style="text-align:center;margin-top:1.5rem;font-size:.75rem;font-family:'Space Mono',monospace;color:{T['subtext']}">
    MedPulse Analytics · Gold Layer · {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} UTC
</div>
""", unsafe_allow_html=True)
 