import streamlit as st
import folium
from streamlit_folium import st_folium
import plotly.graph_objects as go
from datetime import datetime
import pytz
import json, urllib.request
import duckdb
import os

# ── Embedded logo (base64) ────────────────────────────────────────────────────
LOGO_B64 = "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"

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GridGuard – Grid Failure Prediction & Dispatch",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Share+Tech+Mono&family=Exo+2:wght@300;400;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Exo 2', sans-serif; background-color: #0d1b2a; color: #e0eaf5; }
.stApp { background-color: #0d1b2a; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1rem 2rem 2rem 2rem; max-width: 100%; }

.top-header {
    display: flex; align-items: center; justify-content: space-between;
    background: #112236; border-bottom: 2px solid #1e3a5f;
    padding: 0.75rem 1.5rem; border-radius: 8px; margin-bottom: 1.2rem;
}
.logo-title { display: flex; align-items: center; gap: 1rem; }
.logo-img { height: 52px; width: auto; display: block; }
.header-title {
    font-family: 'Rajdhani', sans-serif; font-size: 1.6rem; font-weight: 700;
    letter-spacing: 2px; color: #e0eaf5; text-transform: uppercase;
}
.status-badge-active {
    background: #1b5e20; border: 1px solid #2e7d32; color: #a5d6a7;
    padding: 0.4rem 0.9rem; border-radius: 6px;
    font-family: 'Share Tech Mono', monospace; font-size: 0.78rem;
    text-align: center; line-height: 1.5;
}
.status-badge-inactive {
    background: #7f0000; border: 1px solid #c62828; color: #ef9a9a;
    padding: 0.4rem 0.9rem; border-radius: 6px;
    font-family: 'Share Tech Mono', monospace; font-size: 0.78rem;
    text-align: center; line-height: 1.5;
}
.section-label {
    font-family: 'Rajdhani', sans-serif; font-size: 1.05rem; font-weight: 700;
    letter-spacing: 2px; color: #4fc3f7; text-transform: uppercase;
    margin-bottom: 0.6rem; border-bottom: 1px solid #1e3a5f; padding-bottom: 0.3rem;
}
.sidebar-panel { background: #112236; border: 1px solid #1e3a5f; border-radius: 10px; padding: 1.2rem; }
.kpi-card { border-radius: 8px; padding: 1rem 1.2rem; text-align: center; height: 100%; }
.kpi-card.red   { background: #7f0000; border: 1px solid #c62828; }
.kpi-card.amber { background: #e65100; border: 1px solid #bf360c; }
.kpi-card.green { background: #1b5e20; border: 1px solid #2e7d32; }
.kpi-label { font-size: 0.8rem; color: rgba(255,255,255,0.75); margin-bottom: 0.2rem; }
.kpi-value { font-family: 'Rajdhani', sans-serif; font-size: 2rem; font-weight: 700; color: #fff; }
.kpi-delta { font-family: 'Share Tech Mono', monospace; font-size: 0.78rem; color: rgba(255,255,255,0.7); margin-top: 0.15rem; }
.ctx-item { margin-bottom: 0.75rem; }
.ctx-lbl { font-size: 0.78rem; color: #7ea8c9; margin-bottom: 0.15rem; }
.ctx-val { font-family: 'Rajdhani', sans-serif; font-size: 1.5rem; font-weight: 600; color: #e0eaf5; }
.dispatch-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
.dispatch-table th {
    background: #0d1b2a; color: #7ea8c9; font-family: 'Rajdhani', sans-serif;
    font-weight: 600; letter-spacing: 1px; padding: 0.6rem 0.8rem;
    text-align: left; border-bottom: 2px solid #1e3a5f;
}
.dispatch-table td { padding: 0.55rem 0.8rem; border-bottom: 1px solid #1a2e45; }
.dispatch-table tr:last-child td { border-bottom: none; }
.priority-1 { background: rgba(198,40,40,0.25); }
.priority-2 { background: rgba(230,81,0,0.20); }
.priority-3 { background: rgba(249,168,37,0.15); }
.badge-red    { background:#c62828; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.75rem; }
.badge-amber  { background:#e65100; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.75rem; }
.badge-yellow { background:#f9a825; color:#000; padding:2px 8px; border-radius:4px; font-size:0.75rem; }
.crew-tag { background:#1565c0; color:#90caf9; padding:2px 8px; border-radius:4px; font-size:0.75rem; font-family:'Share Tech Mono', monospace; }
.footer-bar {
    background: #112236; border-top: 1px solid #1e3a5f; border-radius: 8px;
    padding: 0.7rem 1.5rem; display: flex; align-items: center;
    justify-content: space-between; margin-top: 1.5rem; font-size: 0.75rem; color: #4a7090;
}
.footer-sources { display: flex; gap: 2rem; }
.footer-ver { font-family: 'Share Tech Mono', monospace; }
div[data-testid="stSelectbox"] > div > div {
    background: #1a2e45 !important; border: 1px solid #1e3a5f !important;
    color: #e0eaf5 !important; border-radius: 6px !important;
}
label[data-testid="stWidgetLabel"] { color: #b0c8e0 !important; font-size: 0.85rem; }
/* Force map container to fill column */
.stFolium iframe { width: 100% !important; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
WAT = pytz.timezone("Africa/Lagos")
def get_wat_time():
    return datetime.now(WAT).strftime("%H:%M WAT")
def system_is_active():
    return st.session_state.get("system_active", True)
def risk_color(risk):
    if risk >= 60: return "#ef5350"
    elif risk >= 30: return "#ffa726"
    else: return "#66bb6a"

# ── SCADA Context + KPI delta loader ─────────────────────────────────────────
@st.cache_data(ttl=60)
def load_scada_context() -> dict:
    """
    Queries scada_telemetry for live context panel + KPI hour-on-hour deltas.

    Context panel:
      avg_frequency_hz       — AVG(frequency_hz) at the latest timestamp (all corridors)
      min_voltage_kv         — MIN(voltage_kv)   at the latest timestamp
      plant_availability_pct — corridors with NO failure in the last 30 days,
                               expressed as a % of total corridor-hours in that window.
                               This gives a meaningful operational figure rather than
                               a near-100% all-time average dominated by normal hours.

    KPI deltas (current vs 1 hour prior, same latest-timestamp logic):
      delta_at_risk    — change in count of corridors with voltage sag (proxy for risk)
      delta_freq       — change in avg frequency Hz
      delta_voltage    — change in min voltage kV
    """
    db_path = os.environ.get("GRIDGUARD_DB_PATH", "data/gridguard.duckdb")
    fallback = {
        "avg_frequency_hz":       None,
        "min_voltage_kv":         None,
        "plant_availability_pct": None,
        "delta_freq":             None,
        "delta_voltage":          None,
        "delta_at_risk":          None,
        "latest_ts":              None,
    }
    try:
        con = duckdb.connect(db_path, read_only=True)

        # ── 1. Latest & previous timestamps ──────────────────────────────────
        ts_row = con.execute("""
            SELECT
                MAX(timestamp) AS t_now,
                MAX(timestamp) - INTERVAL 1 HOUR AS t_prev
            FROM scada_telemetry
        """).fetchone()
        if not ts_row or ts_row[0] is None:
            con.close()
            return fallback

        t_now, t_prev = ts_row[0], ts_row[1]

        # ── 2. Current-hour snapshot ──────────────────────────────────────────
        now_row = con.execute("""
            SELECT
                ROUND(AVG(frequency_hz), 2) AS avg_freq,
                ROUND(MIN(voltage_kv),   2) AS min_volt,
                -- Corridors considered "at risk" = voltage dropped below 95% of base
                -- (330kV base → <313.5kV is sag; 132kV base → <125.4kV is sag)
                SUM(CASE
                    WHEN voltage_kv < 313.5 AND voltage_kv > 200 THEN 1  -- 330kV sag
                    WHEN voltage_kv < 125.4 AND voltage_kv <= 200 THEN 1  -- 132kV sag
                    ELSE 0
                END) AS at_risk_count
            FROM scada_telemetry
            WHERE timestamp = ?
        """, [t_now]).fetchone()

        # ── 3. Previous-hour snapshot ─────────────────────────────────────────
        prev_row = con.execute("""
            SELECT
                ROUND(AVG(frequency_hz), 2) AS avg_freq,
                ROUND(MIN(voltage_kv),   2) AS min_volt,
                SUM(CASE
                    WHEN voltage_kv < 313.5 AND voltage_kv > 200 THEN 1
                    WHEN voltage_kv < 125.4 AND voltage_kv <= 200 THEN 1
                    ELSE 0
                END) AS at_risk_count
            FROM scada_telemetry
            WHERE timestamp = ?
        """, [t_prev]).fetchone()

        # ── 4. Plant availability — last 30 days ──────────────────────────────
        # % of corridor-hours with no failure event in the trailing 30-day window.
        # This surfaces actual operational availability, not the all-time near-100%.
        avail_row = con.execute("""
            SELECT
                ROUND(
                    (SUM(CASE WHEN failure_event = 0 THEN 1 ELSE 0 END) * 100.0)
                    / NULLIF(COUNT(*), 0),
                1) AS availability_pct
            FROM scada_telemetry
            WHERE timestamp >= (SELECT MAX(timestamp) - INTERVAL 30 DAY
                                FROM scada_telemetry)
        """).fetchone()

        con.close()

        avg_freq_now  = now_row[0]  if now_row  else None
        min_volt_now  = now_row[1]  if now_row  else None
        at_risk_now   = now_row[2]  if now_row  else None
        avg_freq_prev = prev_row[0] if prev_row else None
        min_volt_prev = prev_row[1] if prev_row else None
        at_risk_prev  = prev_row[2] if prev_row else None
        avail_pct     = avail_row[0] if avail_row else None

        return {
            "avg_frequency_hz":       avg_freq_now,
            "min_voltage_kv":         min_volt_now,
            "plant_availability_pct": avail_pct,
            "delta_freq":    round(avg_freq_now - avg_freq_prev, 3)
                             if avg_freq_now is not None and avg_freq_prev is not None else None,
            "delta_voltage": round(min_volt_now - min_volt_prev, 2)
                             if min_volt_now is not None and min_volt_prev is not None else None,
            "delta_at_risk": int(at_risk_now - at_risk_prev)
                             if at_risk_now is not None and at_risk_prev is not None else None,
            "latest_ts": t_now,
        }

    except Exception as e:
        import logging
        logging.getLogger("gridguard.dashboard").warning(
            "Could not load SCADA context: %s", e
        )
        return fallback

# ── Session state ─────────────────────────────────────────────────────────────
if "system_active" not in st.session_state:
    st.session_state.system_active = True
if "available_crews" not in st.session_state:
    st.session_state.available_crews = 3

# ── Static data + live loaders ────────────────────────────────────────────────
DISCOS = [
    "All 11 Discos", "Abuja DisCo", "Benin DisCo", "Eko DisCo", "Enugu DisCo",
    "Ibadan DisCo", "Ikeja DisCo", "Jos DisCo", "Kaduna DisCo",
    "Kano DisCo", "Port Harcourt DisCo", "Yola DisCo",
]


@st.cache_data(ttl=30)
def load_dispatch_data() -> list:
    """
    Loads live corridor risk data from dispatch_queue.csv (pipeline output)
    and enriches it with physical metadata from the DuckDB corridors table.

    Columns used from DuckDB corridors:
        name, disco_name, latitude, longitude,
        hospital_count, school_count, market_count

    Falls back to demo data if the pipeline has not been run yet so the
    dashboard always renders — even during a live presentation.
    """
    crew_names = ["Alpha", "Beta", "Gamma", "Delta", "Echo", "Zeta", "Eta", "Theta"]
    db_path    = os.environ.get("GRIDGUARD_DB_PATH", "data/gridguard.duckdb")

    try:
        import pandas as pd
        queue_path = "data/processed/dispatch_queue.csv"
        if not os.path.exists(queue_path):
            raise FileNotFoundError("dispatch_queue.csv not found — run the pipeline first")

        df = pd.read_csv(queue_path)

        # ── Enrich with geo + infra counts from DuckDB ──────────────────────
        con = duckdb.connect(db_path, read_only=True)
        meta = con.execute("""
            SELECT
                name,
                disco_name,
                latitude,
                longitude,
                COALESCE(hospital_count, 0) AS hospital_count,
                COALESCE(school_count,   0) AS school_count,
                COALESCE(market_count,   0) AS market_count
            FROM corridors
        """).df()
        con.close()

        # Merge on corridor name
        df = df.rename(columns={"Corridor": "name"})
        df = df.merge(meta, on="name", how="left")

        # ── Issue 3: Format infra counts into human-readable display string ─
        def fmt_infra(row):
            parts = []
            if row.get("hospital_count", 0):
                parts.append(f"Hospital ({int(row['hospital_count'])})")
            if row.get("school_count", 0):
                parts.append(f"School ({int(row['school_count'])})")
            if row.get("market_count", 0):
                parts.append(f"Market ({int(row['market_count'])})")
            return ", ".join(parts) if parts else "Residential"

        df["infra"] = df.apply(fmt_infra, axis=1)

        # ── Rename to internal dict shape ────────────────────────────────────
        df = df.rename(columns={
            "AI Probability (%)": "risk",
            "NGN Loss/hr":        "loss",
            "disco_name":         "disco",
            "latitude":           "lat",
            "longitude":          "lon",
        })

        corridors = []
        for i, row in df.iterrows():
            # disco_name in DB is plain e.g. "Benin" — append " DisCo" only if missing
            disco_raw = str(row.get("disco", ""))
            disco = disco_raw if disco_raw.endswith("DisCo") else disco_raw + " DisCo"
            corridors.append({
                "name":  row.get("name",  "Unknown"),
                "risk":  float(row.get("risk", 0)),
                "loss":  float(row.get("loss", 0)),
                "infra": row.get("infra", "—"),
                "crew":  crew_names[i] if i < len(crew_names) else f"Crew {i+1}",
                "lat":   float(row.get("lat", 9.08)),
                "lon":   float(row.get("lon", 8.67)),
                "disco": disco,
            })
        return corridors

    except Exception as e:
        import logging
        logging.getLogger("gridguard.dashboard").warning(
            "Could not load dispatch data (%s) — using demo data", e
        )
        # Demo fallback — ensures dashboard renders during presentation
        # even if the pipeline hasn't been run yet
        return [
            {"name": "Benin-Onitsha 330kV",  "risk": 78, "loss": 4.2, "infra": "Hospital (1), Market (3)", "crew": "Alpha", "lat": 6.335, "lon": 5.627, "disco": "Benin DisCo"},
            {"name": "Ikeja-Ota 132kV",       "risk": 61, "loss": 2.8, "infra": "School (2)",               "crew": "Beta",  "lat": 6.600, "lon": 3.230, "disco": "Ikeja DisCo"},
            {"name": "Kano-Kaduna 330kV",     "risk": 54, "loss": 1.9, "infra": "Industry Zone",            "crew": "Gamma", "lat": 11.10, "lon": 7.720, "disco": "Kano DisCo"},
            {"name": "Egbin-Lagos 132kV",     "risk": 22, "loss": 0.5, "infra": "Residential",              "crew": "Delta", "lat": 6.545, "lon": 3.715, "disco": "Eko DisCo"},
            {"name": "Shiroro-Kaduna 330kV",  "risk": 41, "loss": 1.1, "infra": "Airport",                  "crew": "Echo",  "lat": 10.50, "lon": 7.100, "disco": "Kaduna DisCo"},
            {"name": "Alaoji-Onitsha 132kV",  "risk": 33, "loss": 0.8, "infra": "Market",                   "crew": "Zeta",  "lat": 5.020, "lon": 7.000, "disco": "Enugu DisCo"},
        ]

# ── TOP HEADER ────────────────────────────────────────────────────────────────
active = system_is_active()
badge_cls    = "status-badge-active"   if active else "status-badge-inactive"
badge_status = "● System Active"       if active else "● System Inactive"

st.markdown(f"""
<div class="top-header">
  <div class="logo-title">
    <img src="data:image/png;base64,{LOGO_B64}" class="logo-img" alt="GridGuard logo">
    <span class="header-title">Grid Failure Prediction &amp; Dispatch System</span>
  </div>
  <div class="{badge_cls}">{badge_status}<br>{get_wat_time()}</div>
</div>
""", unsafe_allow_html=True)

# ── LAYOUT ────────────────────────────────────────────────────────────────────
left_col, main_col = st.columns([1, 3], gap="medium")

# ── LEFT PANEL ────────────────────────────────────────────────────────────────
with left_col:
    st.markdown('<div class="section-label">Filters &amp; Controls</div>', unsafe_allow_html=True)

    selected_disco = st.selectbox("Disco Selector:", DISCOS, index=0)
    time_window    = st.slider("Time Window:", min_value=1, max_value=12, value=(1, 6), format="%dhr")
    risk_threshold = st.slider("Risk Threshold:", min_value=0, max_value=100, value=30, format="%d%%")

    st.markdown("**Available Crews:**")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        if st.button("−", use_container_width=True, key="crew_minus"):
            st.session_state.available_crews = max(0, st.session_state.available_crews - 1)
            st.rerun()
    with c2:
        st.markdown(f"""<div style="text-align:center;background:#1a2e45;border:1px solid #1e3a5f;
            border-radius:6px;padding:0.4rem;font-family:'Rajdhani',sans-serif;
            font-size:1.5rem;font-weight:700;color:#e0eaf5;margin-top:2px;">
            {st.session_state.available_crews}</div>""", unsafe_allow_html=True)
    with c3:
        if st.button("＋", use_container_width=True, key="crew_plus"):
            st.session_state.available_crews += 1
            st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    tog_label = "🔴 Deactivate System" if active else "🟢 Activate System"
    if st.button(tog_label, use_container_width=True):
        st.session_state.system_active = not st.session_state.system_active
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-label">Context</div>', unsafe_allow_html=True)

    # scada is loaded in the main panel block; read it from session state cache here
    _scada = load_scada_context()

    def fmt_ctx(value, unit, decimals=2):
        """Format a context value or show a subtle dash when unavailable."""
        if value is None:
            return f'<span style="color:#4a7090;font-size:1rem;">— unavailable</span>'
        return f'{value:.{decimals}f} {unit}'

    # Colour-code frequency: warn if outside Nigerian Grid Code bands
    freq_val  = _scada["avg_frequency_hz"]
    freq_color = (
        "#ef5350" if freq_val is not None and (freq_val < 49.5 or freq_val > 50.5)
        else "#ffa726" if freq_val is not None and (freq_val < 49.8 or freq_val > 50.2)
        else "#e0eaf5"
    )

    # Min voltage at latest timestamp — data shows 132kV and 330kV corridors
    # Warn if below 95% of 132kV nominal (125.4 kV) since that's the lower nominal
    volt_val   = _scada["min_voltage_kv"]
    volt_color = (
        "#ef5350" if volt_val is not None and volt_val < 118.8   # <90% of 132kV
        else "#ffa726" if volt_val is not None and volt_val < 125.4  # <95% of 132kV
        else "#e0eaf5"
    )

    # Plant availability over last 30 days
    avail_val   = _scada["plant_availability_pct"]
    avail_color = (
        "#ef5350" if avail_val is not None and avail_val < 50
        else "#ffa726" if avail_val is not None and avail_val < 70
        else "#66bb6a"
    )

    # Latest data timestamp for panel subtitle
    latest_ts = _scada.get("latest_ts")
    ts_label  = latest_ts.strftime("%d %b %Y %H:%M") if latest_ts else "—"

    st.markdown(f"""
    <div class="sidebar-panel">
        <div style="font-family:'Share Tech Mono',monospace;font-size:0.7rem;
                    color:#4a7090;margin-bottom:0.8rem;">AS AT {ts_label}</div>
        <div class="ctx-item">
            <div class="ctx-lbl">Avg System Frequency</div>
            <div class="ctx-val" style="color:{freq_color};">
                {fmt_ctx(freq_val, "Hz")}
            </div>
        </div>
        <div class="ctx-item">
            <div class="ctx-lbl">Min Live Voltage</div>
            <div class="ctx-val" style="color:{volt_color};">
                {fmt_ctx(volt_val, "kV")}
            </div>
        </div>
        <div class="ctx-item">
            <div class="ctx-lbl">Plant Availability (30d)</div>
            <div class="ctx-val" style="color:{avail_color};">
                {fmt_ctx(avail_val, "%", decimals=1)}
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

# ── MAIN PANEL ────────────────────────────────────────────────────────────────
with main_col:

    # Load live corridor data from pipeline (dispatch_queue.csv + DuckDB)
    CORRIDORS = load_dispatch_data()

    # Filter by disco + threshold
    if selected_disco == "All 11 Discos":
        filtered = [c for c in CORRIDORS if c["risk"] >= risk_threshold]
    else:
        filtered = [c for c in CORRIDORS if c["risk"] >= risk_threshold and c["disco"] == selected_disco]

    at_risk      = len([c for c in filtered if c["risk"] >= 60])
    highest      = max(filtered, key=lambda x: x["risk"]) if filtered else {"risk": 0, "name": "N/A"}
    est_loss     = sum(c["loss"] for c in filtered if c["risk"] >= 60)
    crews_needed = min(at_risk, st.session_state.available_crews)

    def fmt_delta(val, unit="", decimals=1, invert=False):
        """Arrow + value for KPI delta line. invert=True means higher = worse."""
        if val is None:
            return "— vs last hour"
        direction = val > 0
        if invert:
            direction = not direction
        arrow  = "↑" if val > 0 else ("↓" if val < 0 else "→")
        color  = "#ef5350" if (invert and val > 0) or (not invert and val < 0) else \
                 "#66bb6a" if (invert and val < 0) or (not invert and val > 0) else "#7ea8c9"
        prefix = "+" if val > 0 else ""
        return f'<span style="color:{color}">{arrow} {prefix}{val:.{decimals}f}{unit} vs last hr</span>'

    # ── KPI CARDS ──
    st.markdown('<div class="section-label">Grid Status</div>', unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4, gap="small")

    # Contextual subtexts derived directly from filtered corridor data
    total_monitored  = len(CORRIDORS)
    highest_name     = highest["name"].replace(" 330kV","").replace(" 132kV","")
    high_risk_loss   = sum(c["loss"] for c in filtered if c["risk"] >= 60)
    delta_crew       = crews_needed - st.session_state.available_crews

    with k1:
        st.markdown(f'''<div class="kpi-card red">
            <div class="kpi-label">Corridors at Risk</div>
            <div class="kpi-value">{at_risk}</div>
            <div class="kpi-delta">of {total_monitored} corridors monitored</div>
        </div>''', unsafe_allow_html=True)
    with k2:
        st.markdown(f'''<div class="kpi-card red">
            <div class="kpi-label">Highest Risk Score</div>
            <div class="kpi-value">{highest["risk"]}%</div>
            <div class="kpi-delta" title="{highest["name"]}">{highest_name[:18]}</div>
        </div>''', unsafe_allow_html=True)
    with k3:
        corridor_word = "corridor" if at_risk == 1 else "corridors"
        st.markdown(f'''<div class="kpi-card amber">
            <div class="kpi-label">Est. Loss / Hour</div>
            <div class="kpi-value">&#x20A6;{est_loss:.1f}M</div>
            <div class="kpi-delta">across {at_risk} high-risk {corridor_word}</div>
        </div>''', unsafe_allow_html=True)
    with k4:
        st.markdown(f'''<div class="kpi-card green">
            <div class="kpi-label">Crews Needed</div>
            <div class="kpi-value">{crews_needed}</div>
            <div class="kpi-delta">{delta_crew:+d} vs available</div>
        </div>''', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── MAP + CHART ── equal split for visual balance
    map_col, chart_col = st.columns([1, 1], gap="medium")

    with map_col:
        st.markdown('<div class="section-label">Nigeria Risk Map</div>', unsafe_allow_html=True)

        # Nigeria bounding box: roughly 4°N–14°N, 2°E–15°E
        # Fit bounds to Nigeria so map is always properly scaled
        m = folium.Map(
            location=[9.0820, 8.6753],
            zoom_start=6,
            tiles="CartoDB dark_matter",
            prefer_canvas=True,
            min_zoom=5,
            max_zoom=10,
        )

        # Fit map tightly to Nigeria's extent
        m.fit_bounds([[4.2, 2.7], [13.9, 14.7]])

        # Only plot markers for corridors that pass the filter
        map_corridors = filtered  # already filtered by disco + threshold

        if map_corridors:
            for c in map_corridors:
                color = risk_color(c["risk"])
                folium.CircleMarker(
                    location=[c["lat"], c["lon"]],
                    radius=10 + c["risk"] // 10,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.75,
                    popup=folium.Popup(
                        f"<b>{c['name']}</b><br>Risk: {c['risk']}%<br>Loss/hr: ₦{c['loss']}M",
                        max_width=200,
                    ),
                    tooltip=f"{c['name']} — {c['risk']}%",
                ).add_to(m)

            # Legend only shown when there are markers
            legend_html = """
            <div style="position:fixed;bottom:20px;left:20px;z-index:999;
                        background:#112236cc;border:1px solid #1e3a5f;
                        border-radius:6px;padding:10px 14px;font-size:12px;color:#e0eaf5;">
                <b>Risk Level</b><br>
                <span style="color:#ef5350;">●</span> High (&gt;60%)<br>
                <span style="color:#ffa726;">●</span> Medium (30–60%)<br>
                <span style="color:#66bb6a;">●</span> Low (&lt;30%)
            </div>"""
            m.get_root().html.add_child(folium.Element(legend_html))
        else:
            # No-risk overlay message
            no_risk_html = """
            <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
                        z-index:999;background:#112236ee;border:1px solid #1e3a5f;
                        border-radius:8px;padding:16px 24px;font-size:14px;
                        color:#a5d6a7;text-align:center;font-family:'Rajdhani',sans-serif;
                        font-weight:700;letter-spacing:1px;">
                ✅ NO RISK CORRIDORS DETECTED<br>
                <span style="font-size:11px;font-weight:400;color:#7ea8c9;">
                    All corridors below threshold
                </span>
            </div>"""
            m.get_root().html.add_child(folium.Element(no_risk_html))

        # Key is tied to filter state only — map won't re-render on crew +/- clicks
        map_key = f"risk_map_{selected_disco}_{risk_threshold}_{len(filtered)}"
        st_folium(m, use_container_width=True, height=360, returned_objects=[], key=map_key)

    with chart_col:
        st.markdown('<div class="section-label">Risk Score Chart</div>', unsafe_allow_html=True)

        if filtered:
            chart_data = sorted(filtered, key=lambda x: x["risk"], reverse=True)[:6]
            names  = [c["name"].rsplit(" ", 1)[0] for c in chart_data]
            risks  = [c["risk"] for c in chart_data]
            colors = [risk_color(r) for r in risks]

            fig = go.Figure(go.Bar(
                x=risks, y=names, orientation="h",
                marker=dict(color=colors, line=dict(width=0)),
                text=[f"{r}%" for r in risks],
                textposition="outside",
                textfont=dict(color="#e0eaf5", size=12, family="Rajdhani"),
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=50, t=10, b=10), height=360,
                xaxis=dict(range=[0, 115], showgrid=True, gridcolor="#1e3a5f",
                           color="#7ea8c9", tickfont=dict(family="Share Tech Mono", size=10)),
                yaxis=dict(color="#e0eaf5", tickfont=dict(family="Exo 2", size=11),
                           autorange="reversed"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown("""
            <div style="height:360px;display:flex;align-items:center;justify-content:center;
                        background:#112236;border:1px solid #1e3a5f;border-radius:8px;
                        color:#a5d6a7;font-family:'Rajdhani',sans-serif;font-size:1.1rem;
                        letter-spacing:1px;">✅ No corridors above threshold</div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── DISPATCH QUEUE ──
    st.markdown('<div class="section-label">Dispatch Queue — Ranked by Consequence</div>', unsafe_allow_html=True)

    # Preserve CSV order — already ranked by consequence score (prob × loss × infra)
    # from scoring/dispatch.py. Re-sorting by raw risk% would contradict the header.
    dispatch = filtered  # CSV order = consequence rank

    def priority_badge(risk):
        if risk >= 60: return '<span class="badge-red">▲ HIGH</span>'
        elif risk >= 30: return '<span class="badge-amber">▲ MED</span>'
        return '<span class="badge-yellow">▲ LOW</span>'

    def row_class(i):
        return ["priority-1","priority-2","priority-3"][min(i,2)]

    if dispatch:
        crew_names = ["Alpha", "Beta", "Gamma", "Delta", "Echo", "Zeta", "Eta", "Theta"]
        rows_html = ""
        for i, c in enumerate(dispatch[:5]):
            # Crew assigned by display rank (Alpha=highest priority) not CSV order
            crew_assigned = crew_names[i] if i < st.session_state.available_crews else "—"
            risk_c = '#ef5350' if c["risk"]>=60 else '#ffa726' if c["risk"]>=30 else '#66bb6a'
            rows_html += f"""<tr class="{row_class(i)}">
                <td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:1.1rem;">{i+1}</td>
                <td>{priority_badge(c["risk"])}</td>
                <td style="font-weight:600;">{c["name"]}</td>
                <td><span style="color:{risk_c};font-family:'Rajdhani',sans-serif;font-size:1rem;font-weight:700;">{c["risk"]}%</span></td>
                <td style="font-family:'Share Tech Mono',monospace;">₦{c["loss"]}M</td>
                <td style="font-size:0.8rem;color:#b0c8e0;">{c["infra"]}</td>
                <td><span class="crew-tag">{crew_assigned}</span></td>
            </tr>"""

        st.markdown(f"""
        <div style="background:#112236;border:1px solid #1e3a5f;border-radius:8px;overflow:hidden;padding:0.5rem;">
        <table class="dispatch-table">
            <thead><tr><th>#</th><th>Priority</th><th>Corridor</th>
            <th>Risk</th><th>₦ Loss/hr</th><th>Crit. Infra</th><th>Crew</th></tr></thead>
            <tbody>{rows_html}</tbody>
        </table></div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="padding:1.5rem;background:#112236;border:1px solid #1e3a5f;border-radius:8px;
                    text-align:center;color:#a5d6a7;font-family:'Rajdhani',sans-serif;font-size:1.1rem;
                    letter-spacing:1px;">✅ No dispatch actions required — all corridors within safe parameters</div>
        """, unsafe_allow_html=True)

# ── FOOTER ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="footer-bar">
  <img src="data:image/png;base64,{LOGO_B64}" style="height:28px;width:auto;opacity:0.7;" alt="GridGuard">
  <div class="footer-sources">
    <span>Data Sources:</span><span>NERC Q4 2025</span>
    <span>NASA POWER</span><span>OpenStreetMap</span><span>World Bank</span>
  </div>
  <div class="footer-ver">v1.0</div>
</div>
""", unsafe_allow_html=True)