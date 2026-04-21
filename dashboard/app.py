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

# ── Static data ───────────────────────────────────────────────────────────────
DISCOS = [
    "All 11 Discos", "Abuja DisCo", "Benin DisCo", "Eko DisCo", "Enugu DisCo",
    "Ibadan DisCo", "Ikeja DisCo", "Jos DisCo", "Kaduna DisCo",
    "Kano DisCo", "Port Harcourt DisCo", "Yola DisCo",
]
CORRIDORS = [
    {"name": "Benin-Onitsha 330kV",  "risk": 78, "loss": 4.2, "infra": "Hospital (1)",   "crew": "Alpha", "lat": 6.335, "lon": 5.627, "disco": "Benin DisCo"},
    {"name": "Ikeja-Ota 132kV",      "risk": 61, "loss": 2.8, "infra": "School (2)",     "crew": "Beta",  "lat": 6.600, "lon": 3.230, "disco": "Ikeja DisCo"},
    {"name": "Kano-Kaduna 330kV",    "risk": 54, "loss": 1.9, "infra": "Industry Zone",  "crew": "Gamma", "lat": 11.10, "lon": 7.720, "disco": "Kano DisCo"},
    {"name": "Egbin-Lagos 132kV",    "risk": 22, "loss": 0.5, "infra": "Residential",    "crew": "Delta", "lat": 6.545, "lon": 3.715, "disco": "Eko DisCo"},
    {"name": "Shiroro-Kaduna 330kV", "risk": 41, "loss": 1.1, "infra": "Airport",        "crew": "Echo",  "lat": 10.50, "lon": 7.100, "disco": "Kaduna DisCo"},
    {"name": "Alaoji-Onitsha 132kV", "risk": 33, "loss": 0.8, "infra": "Market",         "crew": "Zeta",  "lat": 5.020, "lon": 7.000, "disco": "Enugu DisCo"},
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
        if st.button("−", use_container_width=True):
            st.session_state.available_crews = max(0, st.session_state.available_crews - 1)
    with c2:
        st.markdown(f"""<div style="text-align:center;background:#1a2e45;border:1px solid #1e3a5f;
            border-radius:6px;padding:0.4rem;font-family:'Rajdhani',sans-serif;
            font-size:1.5rem;font-weight:700;color:#e0eaf5;margin-top:2px;">
            {st.session_state.available_crews}</div>""", unsafe_allow_html=True)
    with c3:
        if st.button("+", use_container_width=True):
            st.session_state.available_crews += 1

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

    # Filter by disco + threshold
    if selected_disco == "All 11 Discos":
        filtered = [c for c in CORRIDORS if c["risk"] >= risk_threshold]
    else:
        filtered = [c for c in CORRIDORS if c["risk"] >= risk_threshold and c["disco"] == selected_disco]

    at_risk      = len([c for c in filtered if c["risk"] >= 60])
    highest      = max(filtered, key=lambda x: x["risk"]) if filtered else {"risk": 0, "name": "N/A"}
    est_loss     = sum(c["loss"] for c in filtered if c["risk"] >= 60)
    crews_needed = min(at_risk, st.session_state.available_crews)

    # Pull SCADA data here so KPI deltas can use it
    scada = load_scada_context()

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
    with k1:
        st.markdown(f'''<div class="kpi-card red">
            <div class="kpi-label">Corridors at Risk</div>
            <div class="kpi-value">{at_risk}</div>
            <div class="kpi-delta">{fmt_delta(scada["delta_at_risk"], "", 0, invert=True)}</div>
        </div>''', unsafe_allow_html=True)
    with k2:
        st.markdown(f'''<div class="kpi-card red">
            <div class="kpi-label">Highest Risk Score</div>
            <div class="kpi-value">{highest["risk"]}%</div>
            <div class="kpi-delta">{fmt_delta(scada["delta_freq"], " Hz", 3)}</div>
        </div>''', unsafe_allow_html=True)
    with k3:
        st.markdown(f'''<div class="kpi-card amber">
            <div class="kpi-label">Est. Loss / Hour</div>
            <div class="kpi-value">₦{est_loss:.1f}M</div>
            <div class="kpi-delta">{fmt_delta(scada["delta_voltage"], " kV", 1)}</div>
        </div>''', unsafe_allow_html=True)
    with k4:
        delta_crew = crews_needed - st.session_state.available_crews
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

        st_folium(m, use_container_width=True, height=360, returned_objects=[])

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

    dispatch = sorted(filtered, key=lambda x: x["risk"], reverse=True)

    def priority_badge(risk):
        if risk >= 60: return '<span class="badge-red">▲ HIGH</span>'
        elif risk >= 30: return '<span class="badge-amber">▲ MED</span>'
        return '<span class="badge-yellow">▲ LOW</span>'

    def row_class(i):
        return ["priority-1","priority-2","priority-3"][min(i,2)]

    if dispatch:
        rows_html = ""
        for i, c in enumerate(dispatch[:5]):
            crew_assigned = c["crew"] if i < st.session_state.available_crews else "—"
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