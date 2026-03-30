"""
LeadHarvest — Streamlit Frontend

Connects to the FastAPI backend at http://127.0.0.1:8000 and provides a
browser UI for triggering scrape jobs, monitoring progress, viewing results,
and downloading the Excel export.

Prerequisites:
    Start the FastAPI backend first:
        venv/Scripts/python.exe -m uvicorn api:app --reload

Then run this app:
    venv/Scripts/python.exe -m streamlit run streamlit_app.py
"""

import os
import time

import httpx
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from db.database import (
    get_available_count,
    get_campaign_stats,
    get_contact_stats,
    get_all_contacts,
    get_enrichment_status,
    get_leads_for_campaign,
    get_opened_leads,
    save_campaign_send,
    get_all_drafts,
    get_draft_stats,
    mark_draft_sent,
    delete_draft,
    get_campaign_status_map,
)
from emailer.sender import send_email
from emailer.templates import render as render_template
from main import refresh_master_excel_campaign_status, rebuild_master_excel_from_db
from utils.timezone_utils import (
    REGION_COUNTRIES,
    SCRAPE_COUNTRIES,
    get_region_work_status,
    is_work_hours,
)

# ── Constants ──────────────────────────────────────────────────────────────────

API_BASE = "http://127.0.0.1:8000"
# API_BASE = "https://lead-harverster-api.onrender.com"
POLL_INTERVAL_SECONDS = 3


# ── Page config (must be first Streamlit call) ─────────────────────────────────

st.set_page_config(
    page_title="LeadHarvest",
    page_icon="L",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── Global CSS ─────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* ---- Hide Streamlit chrome ---- */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .stDeployButton { display: none; }

    /* ---- Page container ---- */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 4rem;
        max-width: 820px;
    }

    /* ---- Hero header ---- */
    .lh-hero {
        text-align: center;
        padding: 2.75rem 0 2.25rem 0;
        margin-bottom: 2rem;
        border-bottom: 1px solid #eef0f6;
    }
    .lh-hero-badge {
        display: inline-block;
        background: #eff6ff;
        color: #2E5FA3;
        border: 1px solid #dbeafe;
        border-radius: 20px;
        padding: 0.2rem 0.85rem;
        font-size: 0.73rem;
        font-weight: 700;
        letter-spacing: 1.2px;
        text-transform: uppercase;
        margin-bottom: 0.85rem;
    }
    .lh-hero-title {
        font-size: 3rem;
        font-weight: 800;
        color: #1e3a5f;
        letter-spacing: -1.5px;
        line-height: 1;
        margin: 0 0 0.5rem 0;
    }
    .lh-hero-title span {
        color: #2E5FA3;
    }
    .lh-hero-sub {
        font-size: 1rem;
        color: #6b7280;
        margin: 0;
        font-weight: 400;
    }

    /* ---- Section headings ---- */
    .lh-section-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #1e3a5f;
        margin: 0 0 0.25rem 0;
    }
    .lh-section-sub {
        font-size: 0.88rem;
        color: #6b7280;
        margin: 0 0 1.5rem 0;
    }

    /* ---- Cards ---- */
    .lh-card {
        background: #ffffff;
        border: 1px solid #e8ecf4;
        border-radius: 16px;
        padding: 1.75rem 2rem 1.5rem 2rem;
        margin-bottom: 1.25rem;
        box-shadow: 0 1px 10px rgba(0, 0, 0, 0.05);
    }

    /* ---- Metric grid ---- */
    .lh-metrics {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 0.65rem;
        margin-bottom: 0.5rem;
    }
    .lh-metric {
        background: #f8faff;
        border: 1px solid #e5ecf8;
        border-radius: 12px;
        padding: 1rem 0.6rem;
        text-align: center;
    }
    .lh-metric-value {
        font-size: 1.65rem;
        font-weight: 800;
        color: #1e3a5f;
        line-height: 1.1;
        margin-bottom: 0.3rem;
    }
    .lh-metric-label {
        font-size: 0.69rem;
        color: #9ca3af;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.6px;
    }
    .lh-metric.hot .lh-metric-value { color: #c2410c; }
    .lh-metric.hot { background: #fff7ed; border-color: #fed7aa; }
    .lh-metric.good .lh-metric-value { color: #15803d; }
    .lh-metric.good { background: #f0fdf4; border-color: #bbf7d0; }

    /* ---- Progress ---- */
    .lh-progress-pct {
        font-size: 3.75rem;
        font-weight: 800;
        color: #2E5FA3;
        line-height: 1;
        text-align: center;
        padding: 1rem 0 0.25rem 0;
    }
    .lh-progress-stage {
        text-align: center;
        font-size: 0.92rem;
        color: #6b7280;
        font-weight: 500;
        margin-bottom: 1.25rem;
    }
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #2E5FA3 0%, #4a85d4 100%) !important;
        border-radius: 4px !important;
    }

    /* ---- Stage pill ---- */
    .lh-stage {
        display: inline-block;
        background: #eff6ff;
        color: #2E5FA3;
        border: 1px solid #dbeafe;
        border-radius: 20px;
        padding: 0.28rem 0.85rem;
        font-size: 0.82rem;
        font-weight: 600;
    }

    /* ---- Info callout ---- */
    .lh-info {
        background: #f0f9ff;
        border-left: 3px solid #38bdf8;
        border-radius: 0 8px 8px 0;
        padding: 0.75rem 1rem;
        font-size: 0.88rem;
        color: #0c4a6e;
        margin: 1.25rem 0;
        line-height: 1.5;
    }

    /* ---- Download highlight ---- */
    .lh-download-card {
        background: linear-gradient(135deg, #f0f7ff 0%, #e8f0fe 100%);
        border: 1px solid #c7d7f5;
        border-radius: 14px;
        padding: 1.25rem 1.5rem;
        margin-bottom: 0.5rem;
    }
    .lh-download-title {
        font-size: 0.95rem;
        font-weight: 700;
        color: #1e3a5f;
        margin-bottom: 0.2rem;
    }
    .lh-download-sub {
        font-size: 0.8rem;
        color: #4b6cb7;
        margin-bottom: 0.85rem;
    }

    /* ---- Buttons ---- */
    .stButton > button[kind="primary"] {
        background: #2E5FA3 !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        padding: 0.65rem 2rem !important;
        box-shadow: 0 4px 14px rgba(46, 95, 163, 0.28) !important;
        transition: all 0.2s ease !important;
        width: 100% !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: #1a3d6b !important;
        box-shadow: 0 6px 20px rgba(46, 95, 163, 0.38) !important;
        transform: translateY(-1px) !important;
    }
    .stButton > button[kind="secondary"] {
        border-radius: 8px !important;
        font-weight: 500 !important;
        color: #374151 !important;
    }

    /* ---- Download button ---- */
    .stDownloadButton > button {
        background: #2E5FA3 !important;
        color: white !important;
        border: none !important;
        border-radius: 9px !important;
        font-weight: 600 !important;
        padding: 0.55rem 1.5rem !important;
        box-shadow: 0 3px 10px rgba(46, 95, 163, 0.25) !important;
    }
    .stDownloadButton > button:hover {
        background: #1a3d6b !important;
        transform: translateY(-1px) !important;
    }

    /* ---- Inputs ---- */
    .stTextInput label {
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        color: #374151 !important;
    }
    .stTextInput > div > div > input {
        border-radius: 9px !important;
        border-color: #d1d9e6 !important;
        font-size: 0.95rem !important;
    }
    .stTextInput > div > div > input:focus {
        border-color: #2E5FA3 !important;
        box-shadow: 0 0 0 3px rgba(46, 95, 163, 0.12) !important;
    }

    /* ---- Divider ---- */
    hr { border: none; border-top: 1px solid #eef0f6; margin: 1.75rem 0; }

    /* ---- Filter row ---- */
    .lh-filter-row {
        background: #f9fafb;
        border: 1px solid #e9ecf0;
        border-radius: 10px;
        padding: 0.85rem 1rem 0.4rem 1rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state initialisation ───────────────────────────────────────────────

_STATE_DEFAULTS: dict = {
    "job_id": None,
    "job_status": "idle",
    "job_category": "",
    "job_city": "",
    "results": None,
    "summary": None,
    "error": None,
    "last_progress": {"current": 0, "total": 0, "stage": "pending"},
    "export_bytes": None,
    "export_filename": "",
}

for _k, _v in _STATE_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── API helpers ────────────────────────────────────────────────────────────────

def _get(path: str) -> httpx.Response | None:
    try:
        return httpx.get(f"{API_BASE}{path}", timeout=12)
    except (httpx.ConnectError, httpx.TimeoutException):
        return None
    except Exception:
        return None


def _post(path: str, payload: dict) -> httpx.Response | None:
    try:
        return httpx.post(f"{API_BASE}{path}", json=payload, timeout=12)
    except (httpx.ConnectError, httpx.TimeoutException):
        return None
    except Exception:
        return None


@st.cache_data(ttl=300)
def _fetch_categories() -> list[str]:
    r = _get("/categories")
    if r and r.status_code == 200:
        return r.json().get("categories", [])
    return [
        "Law Firms", "Photography Studios", "Event Planning Companies",
        "Real Estate Agencies", "Hotels and Hospitality",
        "Restaurants and Food Businesses", "Medical and Dental Clinics",
        "Hair and Beauty Salons", "Logistics and Courier Companies",
        "Churches and Religious Organisations", "Schools and Educational Centres",
        "Fashion and Clothing Brands",
    ]


# ── UI helpers ─────────────────────────────────────────────────────────────────

def _progress_fraction(progress: dict, status: str) -> float:
    if status == "completed":
        return 1.0
    stage = progress.get("stage", "pending")
    if stage in ("pending", ""):
        return 0.02
    if stage == "searching_places":
        return 0.10
    if stage == "scraping_websites":
        total = progress.get("total", 0)
        current = progress.get("current", 0)
        frac = (current / total) if total > 0 else 0.0
        return 0.15 + 0.75 * frac
    if stage == "exporting":
        return 0.95
    return 0.02


def _stage_label(progress: dict, status: str) -> str:
    if status == "completed":
        return "Scrape complete"
    stage = progress.get("stage", "")
    current = progress.get("current", 0)
    total = progress.get("total", 0)
    labels = {
        "pending":           "Preparing...",
        "searching_places":  "Searching Google Places...",
        "scraping_websites": f"Scraping websites  ({current} of {total})",
        "exporting":         "Exporting to Excel...",
        "done":              "Scrape complete",
    }
    return labels.get(stage, "Working...")


def _reset_to_idle() -> None:
    for k, v in _STATE_DEFAULTS.items():
        st.session_state[k] = v


# ── Hero header (shown on every view) ─────────────────────────────────────────

st.markdown("""
<div class="lh-hero">
    <div class="lh-hero-badge">Nigerian SMB Lead Finder</div>
    <div class="lh-hero-title">Lead<span>Harvest</span></div>
    <p class="lh-hero-sub">
        Find businesses, extract contacts, score websites &mdash; all in one run.
    </p>
</div>
""", unsafe_allow_html=True)


# ── Tabs ───────────────────────────────────────────────────────────────────────

tab_scrape, tab_campaigns, tab_dm = st.tabs(["Scrape", "Campaigns", "Decision Makers"])




# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Scrape
# ══════════════════════════════════════════════════════════════════════════════

with tab_scrape:

    # ── Backend connectivity check ──────────────────────────────────────────

    _root = _get("/")
    if _root is None or _root.status_code != 200:
        st.error(
            "**Cannot reach the LeadHarvest API.**\n\n"
            "Start the backend in a separate terminal, then refresh this page:\n"
            "```\nvenv/Scripts/python.exe run_api.py\n```"
        )
        st.stop()

    # ── VIEW 1 — Idle / form ────────────────────────────────────────────────

    if st.session_state.job_status == "idle":

        categories = _fetch_categories()

        st.markdown('<p class="lh-section-title">New Scrape Job</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="lh-section-sub">Enter a business type and city. '
            'Results are exported to Excel automatically.</p>',
            unsafe_allow_html=True,
        )

        category = st.text_input(
            "Business category",
            placeholder="e.g. Law Firms, pharmacy, gym, car dealer, event planner",
            help=(
                "Preset names like 'Law Firms' are mapped to optimised keywords. "
                "Anything else is sent directly to Google Places."
            ),
        )

        with st.expander("Browse 12 preset categories"):
            col_a, col_b = st.columns(2)
            half = len(categories) // 2
            with col_a:
                for c in categories[:half]:
                    st.markdown(f"- {c}")
            with col_b:
                for c in categories[half:]:
                    st.markdown(f"- {c}")

        scrape_col1, scrape_col2 = st.columns(2)
        with scrape_col1:
            city = st.text_input(
                "City",
                placeholder="e.g. Lagos, New York, Tokyo, Mumbai",
            )
        with scrape_col2:
            country = st.selectbox(
                "Country",
                options=["Nigeria"] + [c for c in SCRAPE_COUNTRIES if c != "Nigeria"],
                index=0,
            )

        st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)
        run_clicked = st.button("Run Scrape", type="primary", use_container_width=True)

        if run_clicked:
            if not category.strip():
                st.warning("Please enter a business category.")
                st.stop()
            if not city.strip():
                st.warning("Please enter a city name.")
                st.stop()

            with st.spinner("Starting scrape job..."):
                resp = _post("/scrape", {
                    "category": category.strip(),
                    "city": city.strip(),
                    "country": country,
                })

            if resp is None:
                st.error("Could not connect to the API. Is the backend running?")
                st.stop()

            if resp.status_code == 202:
                body = resp.json()
                st.session_state.job_id = body["job_id"]
                st.session_state.job_status = "pending"
                st.session_state.job_category = category.strip()
                st.session_state.job_city = city.strip()
                st.rerun()
            else:
                body = resp.json()
                st.error(
                    f"The API rejected the request (HTTP {resp.status_code}):  \n"
                    f"{body.get('error', resp.text)}"
                )

    # ── VIEW 2 — Job running / polling ──────────────────────────────────────

    elif st.session_state.job_status in ("pending", "running"):

        job_id = st.session_state.job_id
        poll_resp = _get(f"/results/{job_id}")

        if poll_resp is None:
            st.error(
                "Lost connection to the API while polling. "
                "Check that the backend is still running, then refresh the page."
            )
            st.stop()

        data = poll_resp.json()
        status = data.get("status", "unknown")
        progress = data.get("progress", st.session_state.last_progress)

        st.session_state.last_progress = progress
        st.session_state.job_status = status

        if status == "completed":
            st.session_state.results = data.get("results", [])
            st.session_state.summary = data.get("summary", {})
            st.rerun()

        elif status == "failed":
            st.session_state.error = data.get("error", "Unknown error.")
            st.rerun()

        else:
            category_lbl = data.get("category", st.session_state.job_category)
            city_lbl = data.get("city", st.session_state.job_city)
            stage_txt = _stage_label(progress, status)
            pct = _progress_fraction(progress, status)
            pct_display = max(2, int(pct * 100))

            st.markdown('<p class="lh-section-title">Scrape in Progress</p>', unsafe_allow_html=True)

            st.markdown(
                f'<div class="lh-progress-pct">{pct_display}%</div>'
                f'<div class="lh-progress-stage">{stage_txt}</div>',
                unsafe_allow_html=True,
            )
            st.progress(pct)

            st.markdown("<div style='height: 0.75rem'></div>", unsafe_allow_html=True)

            dcol1, dcol2, dcol3 = st.columns(3)
            dcol1.metric("Category", category_lbl)
            dcol2.metric("City", city_lbl)
            done = progress.get("current", 0)
            total_sites = progress.get("total", 0)
            dcol3.metric("Websites", f"{done} / {total_sites}" if total_sites else "—")

            st.markdown(
                f'<div class="lh-info">'
                f'Scraping typically takes <strong>5 to 15 minutes</strong> depending on '
                f'how many businesses are found and how quickly their sites respond. '
                f'This page refreshes every {POLL_INTERVAL_SECONDS} seconds automatically.'
                f'</div>',
                unsafe_allow_html=True,
            )

            with st.expander("Job details", expanded=False):
                st.json({"job_id": job_id, "status": status, "progress": progress})

            if st.button("Cancel and start over", type="secondary"):
                _reset_to_idle()
                st.rerun()

            time.sleep(POLL_INTERVAL_SECONDS)
            st.rerun()

    # ── VIEW 3 — Failed ─────────────────────────────────────────────────────

    elif st.session_state.job_status == "failed":

        st.error(
            f"**Scrape job failed.**\n\n"
            f"{st.session_state.error or 'An unknown error occurred.'}"
        )
        st.markdown(
            "**Common causes:**\n"
            "- Google Places API key missing or invalid — check your `.env` file\n"
            "- API quota exceeded — wait a few minutes and try again\n"
            "- Network error during scraping"
        )
        st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)
        if st.button("Try Again", type="primary"):
            _reset_to_idle()
            st.rerun()

    # ── VIEW 4 — Results ────────────────────────────────────────────────────

    elif st.session_state.job_status == "completed":

        summary = st.session_state.summary or {}
        results = st.session_state.results or []
        job_id = st.session_state.job_id

        total       = summary.get("total", len(results))
        with_email  = summary.get("with_email", 0)
        with_wa     = summary.get("with_whatsapp", 0)
        high_pri    = summary.get("high_priority", 0)
        avg         = summary.get("avg_quality_score", 0.0)

        st.markdown(
            f'<p class="lh-section-title">'
            f'{st.session_state.job_category} &mdash; {st.session_state.job_city}'
            f'</p>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p class="lh-section-sub">'
            'High priority leads have no website, or a quality score under 50 &mdash; '
            'they need your services the most.'
            '</p>',
            unsafe_allow_html=True,
        )

        st.markdown(f"""
<div class="lh-metrics">
    <div class="lh-metric">
        <div class="lh-metric-value">{total}</div>
        <div class="lh-metric-label">Total Found</div>
    </div>
    <div class="lh-metric good">
        <div class="lh-metric-value">{with_email}</div>
        <div class="lh-metric-label">With Email</div>
    </div>
    <div class="lh-metric good">
        <div class="lh-metric-value">{with_wa}</div>
        <div class="lh-metric-label">WhatsApp</div>
    </div>
    <div class="lh-metric hot">
        <div class="lh-metric-value">{high_pri}</div>
        <div class="lh-metric-label">High Priority</div>
    </div>
    <div class="lh-metric">
        <div class="lh-metric-value">{avg:.0f}</div>
        <div class="lh-metric-label">Avg Score</div>
    </div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div style='height: 0.25rem'></div>", unsafe_allow_html=True)

        if st.session_state.export_bytes is None:
            with st.spinner("Preparing Excel file..."):
                export_resp = _get(f"/export/{job_id}")
            if export_resp and export_resp.status_code == 200:
                st.session_state.export_bytes = export_resp.content
                cd = export_resp.headers.get("content-disposition", "")
                raw_name = (
                    cd.split("filename=")[-1].strip().strip('"')
                    if "filename=" in cd
                    else "leadharvest_results.xlsx"
                )
                st.session_state.export_filename = raw_name
            else:
                st.warning("Excel file could not be retrieved from the API.")

        st.divider()

        if st.session_state.export_bytes:
            st.markdown(
                '<div class="lh-download-card">'
                '<div class="lh-download-title">Excel Export Ready</div>'
                '<div class="lh-download-sub">'
                'Three sheets: All Results, High Priority Leads, Summary'
                '</div>',
                unsafe_allow_html=True,
            )
            st.download_button(
                label="Download Excel File",
                data=st.session_state.export_bytes,
                file_name=st.session_state.export_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()

        st.markdown(
            f'<p class="lh-section-title">'
            f'Results &nbsp;<span style="font-weight:400; color:#9ca3af;">'
            f'{len(results)} businesses</span></p>',
            unsafe_allow_html=True,
        )

        if not results:
            st.info("No businesses were found for this category and city.")
        else:
            st.markdown('<div class="lh-filter-row">', unsafe_allow_html=True)
            fcol1, fcol2, fcol3 = st.columns([1, 1, 2])
            with fcol1:
                only_priority = st.checkbox("High priority only", value=False)
            with fcol2:
                only_email = st.checkbox("With email only", value=False)
            with fcol3:
                name_filter = st.text_input(
                    "search",
                    placeholder="Filter by business name...",
                    label_visibility="collapsed",
                )
            st.markdown("</div>", unsafe_allow_html=True)

            filtered = results
            if only_priority:
                filtered = [r for r in filtered if r.get("High Priority Lead") == "Yes"]
            if only_email:
                filtered = [r for r in filtered if r.get("Email")]
            if name_filter.strip():
                term = name_filter.strip().lower()
                filtered = [r for r in filtered if term in (r.get("Business Name") or "").lower()]

            st.caption(f"Showing **{len(filtered)}** of **{len(results)}** businesses")

            TABLE_COLS = [
                "Business Name", "Has Website", "Phone", "Email", "Email Source",
                "WhatsApp", "Website URL", "Google Rating", "Website Quality Score",
                "High Priority Lead",
            ]
            display_rows = [
                {col: row.get(col, "") for col in TABLE_COLS}
                for row in filtered
            ]

            if display_rows:
                st.dataframe(
                    display_rows,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Business Name": st.column_config.TextColumn("Business Name", width="large"),
                        "Has Website": st.column_config.TextColumn("Has Website", width="small"),
                        "Email Source": st.column_config.TextColumn(
                            "Email Source",
                            help="Where the email was found: website, facebook, instagram, or twitter.",
                            width="small",
                        ),
                        "Website URL": st.column_config.LinkColumn("Website", display_text="Visit site"),
                        "Google Rating": st.column_config.NumberColumn("Rating", format="%.1f"),
                        "Website Quality Score": st.column_config.ProgressColumn(
                            "Quality Score",
                            help="0-100. Lower = weaker website = stronger lead.",
                            min_value=0,
                            max_value=100,
                            format="%d",
                        ),
                        "High Priority Lead": st.column_config.TextColumn(
                            "Priority",
                            help="Yes = score under 50.",
                            width="small",
                        ),
                    },
                )
            else:
                st.info("No results match the current filters.")

        st.divider()

        if st.button("Start a New Scrape", type="primary", use_container_width=True):
            _reset_to_idle()
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Campaigns
# ══════════════════════════════════════════════════════════════════════════════

with tab_campaigns:

    _EMAIL_DAILY_LIMIT = int(os.getenv("EMAIL_DAILY_LIMIT", "50"))
    _EMAIL_DELAY       = float(os.getenv("EMAIL_DELAY_SECONDS", "3"))

    st.markdown('<p class="lh-section-title">Email Campaigns</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="lh-section-sub">'
        'Send cold outreach emails to scraped leads. Emails only go to leads '
        'currently within business hours (9am–6pm) in their timezone.</p>',
        unsafe_allow_html=True,
    )

    # ── Region work-hours status cards ──────────────────────────────────────

    region_statuses = get_region_work_status()
    card_cols = st.columns(len(region_statuses))
    for col, rs in zip(card_cols, region_statuses):
        if rs["in_work_hours"]:
            bg, border, label_color, status_text = "#f0fdf4", "#bbf7d0", "#15803d", "In work hours"
        else:
            bg, border, label_color, status_text = "#fff1f2", "#fecdd3", "#be123c", "Outside hours"
        col.markdown(
            f"""
            <div style="background:{bg};border:1px solid {border};border-radius:12px;
                        padding:0.85rem 1rem;text-align:center;">
                <div style="font-size:0.7rem;font-weight:700;text-transform:uppercase;
                            color:#9ca3af;letter-spacing:0.6px;">{rs['region']}</div>
                <div style="font-size:1.1rem;font-weight:700;color:{label_color};
                            margin:0.2rem 0;">{rs['local_time']}</div>
                <div style="font-size:0.75rem;color:{label_color};">{status_text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

    # ── Stats row ───────────────────────────────────────────────────────────

    stats = get_campaign_stats()
    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    sc1.metric("Total with Email",  stats["total_with_email"])
    sc2.metric("Already Contacted", stats["already_contacted"])
    sc3.metric("Available to Send", stats["available_to_send"])
    sc4.metric("Sent Today",        stats["sent_today"])
    sc5.metric("Emails Opened",     stats.get("total_opens", 0))

    st.divider()

    # ── Test send ───────────────────────────────────────────────────────────

    st.markdown("**Test Delivery**")
    st.caption(
        "Send a preview email to your own address to confirm delivery "
        "before running a real campaign."
    )

    test_col1, test_col2 = st.columns([3, 1])
    with test_col1:
        test_addr = st.text_input(
            "Your personal email address",
            placeholder="yourname@gmail.com",
            label_visibility="visible",
        )
    with test_col2:
        st.markdown("<div style='height: 1.85rem'></div>", unsafe_allow_html=True)
        test_clicked = st.button("Send Test", type="secondary", use_container_width=True)

    if test_clicked:
        if not test_addr.strip() or "@" not in test_addr:
            st.warning("Enter a valid email address to test.")
        else:
            subject, body = render_template("Bluehydra Labs")
            with st.spinner(f"Sending test email to {test_addr}..."):
                ok, reason = send_email(test_addr.strip(), subject, body)
            if ok:
                save_campaign_send(test_addr.strip(), "Bluehydra Labs", "sent")
                st.success(f"Test email sent to {test_addr}. Check your inbox (and spam folder).")
            else:
                st.error(f"Send failed: {reason}")
                st.info(
                    "Check that SMTP_USER, SMTP_PASSWORD, SMTP_HOST are correct in your `.env` file."
                )

    st.divider()

    # ── Email preview ───────────────────────────────────────────────────────

    with st.expander("Preview email template"):
        prev_subject, prev_body = render_template("Bluehydra Labs")
        st.markdown(f"**Subject:** {prev_subject}")
        st.text(prev_body)

    st.divider()

    # ── Campaign send ───────────────────────────────────────────────────────

    st.markdown("**Send Campaign**")

    # Region filter
    region_options = ["All"] + list(REGION_COUNTRIES.keys())
    selected_region = st.selectbox(
        "Target region",
        options=region_options,
        index=0,
        help="Filter leads by region. Only leads currently in work hours (9am-6pm) will be sent to.",
    )

    available = get_available_count(selected_region)
    remaining_today = max(0, _EMAIL_DAILY_LIMIT - stats["sent_today"])

    if selected_region != "All":
        st.caption(f"{available} unsent leads in {selected_region} region.")

    if available == 0:
        st.info(
            f"No leads available for '{selected_region}'. "
            "Run a scrape for this region first, or choose a different region."
        )
    elif remaining_today == 0:
        st.warning(
            f"Daily limit of {_EMAIL_DAILY_LIMIT} emails already reached for today. "
            "Come back tomorrow."
        )
    else:
        batch_max = min(available, remaining_today)
        if batch_max == 1:
            batch_size = 1
            st.info("1 email available to send.")
        else:
            batch_size = st.slider(
                "Emails to send in this batch",
                min_value=1,
                max_value=batch_max,
                value=min(10, batch_max),
                help=f"Daily limit: {_EMAIL_DAILY_LIMIT}. Sent today: {stats['sent_today']}. Available: {available}.",
            )

        send_clicked = st.button(
            f"Send to {batch_size} Lead{'s' if batch_size > 1 else ''}",
            type="primary",
            use_container_width=True,
        )

        if send_clicked:
            leads = get_leads_for_campaign(limit=batch_size, region=selected_region)

            if not leads:
                st.warning("No leads found to send to.")
            else:
                progress_bar = st.progress(0.0)
                status_text  = st.empty()
                sent = failed = skipped = 0
                skipped_list: list[str] = []
                failed_list:  list[str] = []
                sent_list:    list[str] = []

                for i, lead in enumerate(leads):
                    label = lead["business_name"] or lead["email"]

                    # Timezone check — skip if outside business hours
                    if not is_work_hours(lead["timezone"]):
                        skipped += 1
                        skipped_list.append(
                            f"{label} ({lead['country']} — currently outside 9am-6pm)"
                        )
                        progress_bar.progress((i + 1) / len(leads))
                        status_text.text(f"Checking {i + 1} of {len(leads)}: {label}")
                        continue

                    status_text.text(f"Sending {i + 1} of {len(leads)}: {label}")
                    subject, body = render_template(lead["business_name"])
                    ok, _ = send_email(lead["email"], subject, body)

                    if ok:
                        save_campaign_send(lead["email"], lead["business_name"], "sent")
                        sent += 1
                        sent_list.append(label)
                    else:
                        save_campaign_send(lead["email"], lead["business_name"], "failed")
                        failed += 1
                        failed_list.append(label)

                    progress_bar.progress((i + 1) / len(leads))

                    if i < len(leads) - 1:
                        time.sleep(_EMAIL_DELAY)

                status_text.empty()
                progress_bar.progress(1.0)
                refresh_master_excel_campaign_status()

                # ── Results summary ──────────────────────────────────────
                r1, r2, r3 = st.columns(3)
                r1.metric("Sent", sent)
                r2.metric("Skipped (out of hours)", skipped)
                r3.metric("Failed", failed)

                if sent_list:
                    with st.expander(f"Sent ({len(sent_list)})"):
                        for name in sent_list:
                            st.markdown(f"- {name}")

                if skipped_list:
                    with st.expander(f"Skipped — outside work hours ({len(skipped_list)}) — will retry next send"):
                        for name in skipped_list:
                            st.markdown(f"- {name}")

                if failed_list:
                    with st.expander(f"Failed ({len(failed_list)})"):
                        for name in failed_list:
                            st.markdown(f"- {name}")

                if sent == 0 and skipped == 0:
                    st.error("All sends failed. Check your SMTP settings in `.env`.")

                st.rerun()

    st.divider()

    # ── Who opened ──────────────────────────────────────────────────────────

    st.markdown("**Who Opened Your Emails**")
    opened = get_opened_leads()
    if not opened:
        st.info("No opens tracked yet. Opens will appear here once recipients view your emails.")
    else:
        import pandas as pd
        df_opens = pd.DataFrame(opened)
        df_opens.columns = ["Email", "Business Name", "Sent At", "Opened At"]
        st.dataframe(df_opens, use_container_width=True, hide_index=True)

    st.divider()

    # ── Rebuild master Excel ─────────────────────────────────────────────────

    st.markdown("**Rebuild Master Excel from Database**")
    st.caption("Use this to recover the master Excel file if it was deleted or corrupted. Pulls all data from SQLite.")
    if st.button("Rebuild Master Excel", type="secondary"):
        with st.spinner("Rebuilding master Excel from database..."):
            path = rebuild_master_excel_from_db()
        st.success(f"Master Excel rebuilt — {path}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Decision Makers
# ══════════════════════════════════════════════════════════════════════════════

with tab_dm:

    st.markdown('<p class="lh-section-title">Decision Makers</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="lh-section-sub">'
        'Find CEOs, MDs, Founders and other key contacts from business team pages. '
        'Candidate emails are generated from name patterns and saved for outreach.</p>',
        unsafe_allow_html=True,
    )

    # ── Stats row ───────────────────────────────────────────────────────────

    dm_stats = get_contact_stats()
    ds1, ds2, ds3 = st.columns(3)
    ds1.metric("Businesses Enriched", dm_stats["businesses_enriched"])
    ds2.metric("Contacts Found",      dm_stats["total_contacts"])
    ds3.metric("Pending",             340 - dm_stats["businesses_enriched"])

    st.divider()

    # ── Enrichment controls ─────────────────────────────────────────────────

    st.markdown("**Run Enrichment**")
    st.caption(
        "Visits team/about pages of Nigerian businesses in your DB, extracts "
        "decision-maker names and titles, and generates candidate emails. "
        "Only processes businesses not yet enriched. Sites that time out are "
        "skipped and retried on the next run."
    )

    enrich_col1, enrich_col2 = st.columns([2, 1])
    with enrich_col1:
        enrich_limit = st.number_input(
            "Number of businesses to enrich",
            min_value=1, max_value=340, value=10, step=10,
            help="Each business takes 15-60 seconds depending on site speed.",
        )
    with enrich_col2:
        st.markdown("<div style='height: 1.85rem'></div>", unsafe_allow_html=True)
        enrich_clicked = st.button("Run Enrichment", type="primary", use_container_width=True)

    if enrich_clicked:
        from enricher.enricher import enrich_businesses

        with st.spinner(f"Enriching up to {enrich_limit} businesses... this may take several minutes."):
            result = enrich_businesses(limit=int(enrich_limit))

        ec1, ec2, ec3 = st.columns(3)
        ec1.metric("Processed",    result["businesses_processed"])
        ec2.metric("People Found", result["people_found"])
        ec3.metric("Contacts Saved", result["saved"])

        if result["people_found"] == 0:
            st.info(
                "No decision-maker profiles found on these sites. "
                "Many small Nigerian SMB sites don't have a team page. "
                "Try running again to process more businesses."
            )
        elif result["saved"] > 0:
            st.success(f"{result['saved']} new decision-maker contacts saved.")

        st.rerun()

    st.divider()

    # ── Enrichment status table ─────────────────────────────────────────────

    st.markdown("**Enrichment Status**")
    st.caption("Shows the last 100 Nigerian businesses and their enrichment state.")

    enrich_status = get_enrichment_status(limit=100)

    if enrich_status:
        STATUS_COLS = ["business_name", "status", "contacts_found", "enriched_at", "website_url"]
        status_rows = [{col: r.get(col, "") for col in STATUS_COLS} for r in enrich_status]
        st.dataframe(
            status_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "business_name":  st.column_config.TextColumn("Business",      width="large"),
                "status":         st.column_config.TextColumn("Status",        width="small"),
                "contacts_found": st.column_config.NumberColumn("Contacts",    width="small"),
                "enriched_at":    st.column_config.TextColumn("Enriched At",   width="medium"),
                "website_url":    st.column_config.LinkColumn("Website", display_text="Visit"),
            },
        )

    st.divider()

    # ── Contacts table ──────────────────────────────────────────────────────

    contacts = get_all_contacts()

    st.markdown(
        f'<p class="lh-section-title">'
        f'Contacts &nbsp;<span style="font-weight:400; color:#9ca3af;">'
        f'{len(contacts)} total</span></p>',
        unsafe_allow_html=True,
    )

    if not contacts:
        st.info("No contacts yet. Run enrichment above to find decision-makers.")
    else:
        st.markdown('<div class="lh-filter-row">', unsafe_allow_html=True)
        fc1, fc2 = st.columns([1, 3])
        with fc1:
            dm_status_filter = st.selectbox(
                "Status",
                options=["All", "unverified", "verified", "catch_all"],
                label_visibility="collapsed",
            )
        with fc2:
            dm_name_filter = st.text_input(
                "search",
                placeholder="Filter by name or business...",
                label_visibility="collapsed",
            )
        st.markdown("</div>", unsafe_allow_html=True)

        filtered_contacts = contacts
        if dm_status_filter != "All":
            filtered_contacts = [c for c in filtered_contacts if c["smtp_status"] == dm_status_filter]
        if dm_name_filter.strip():
            term = dm_name_filter.strip().lower()
            filtered_contacts = [
                c for c in filtered_contacts
                if term in (c.get("person_name") or "").lower()
                or term in (c.get("business_name") or "").lower()
            ]

        st.caption(f"Showing **{len(filtered_contacts)}** of **{len(contacts)}** contacts")

        if filtered_contacts:
            DM_COLS = ["person_name", "title", "candidate_email",
                       "business_name", "pattern_used", "source_page_url"]
            display_rows = [{col: c.get(col, "") for col in DM_COLS} for c in filtered_contacts]

            st.dataframe(
                display_rows,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "person_name":     st.column_config.TextColumn("Name",        width="medium"),
                    "title":           st.column_config.TextColumn("Title",       width="medium"),
                    "candidate_email": st.column_config.TextColumn("Email",       width="large"),
                    "business_name":   st.column_config.TextColumn("Business",    width="medium"),
                    "pattern_used":    st.column_config.TextColumn("Pattern",     width="small"),
                    "source_page_url": st.column_config.LinkColumn("Source Page", display_text="View page"),
                },
            )
        else:
            st.info("No contacts match the current filters.")

    st.divider()

    # ── Drafts ──────────────────────────────────────────────────────────────

    draft_stats = get_draft_stats()
    st.markdown(
        f'<p class="lh-section-title">'
        f'Email Drafts &nbsp;<span style="font-weight:400; color:#9ca3af;">'
        f'{draft_stats["total"]} total</span></p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="lh-section-sub">'
        'Gemini writes a personalised cold email for each contact based on their '
        'role and what their company website says. Review drafts here before sending.</p>',
        unsafe_allow_html=True,
    )

    dr1, dr2, dr3 = st.columns(3)
    dr1.metric("Total Drafts",   draft_stats["total"])
    dr2.metric("Pending Send",   draft_stats["pending"])
    dr3.metric("Emails Sent",    draft_stats["emails_sent"])

    st.markdown("<br>", unsafe_allow_html=True)

    draft_col1, draft_col2 = st.columns([2, 1])
    with draft_col1:
        draft_limit = st.number_input(
            "Number of contacts to draft for",
            min_value=1, max_value=500, value=20, step=10,
            help="Gemini writes one email per contact. Each takes ~2 seconds.",
            key="draft_limit",
        )
    with draft_col2:
        st.markdown("<div style='height: 1.85rem'></div>", unsafe_allow_html=True)
        draft_clicked = st.button("Generate Drafts", type="primary", use_container_width=True)

    if draft_clicked:
        from enricher.drafter import generate_drafts
        with st.spinner(f"Generating up to {draft_limit} email drafts with Gemini..."):
            dresult = generate_drafts(limit=int(draft_limit))
        dc1, dc2, dc3 = st.columns(3)
        dc1.metric("Contacts Processed", dresult["contacts_processed"])
        dc2.metric("Drafts Saved",       dresult["drafts_saved"])
        dc3.metric("Skipped",            dresult["skipped"])
        if dresult["drafts_saved"] > 0:
            st.success(f"{dresult['drafts_saved']} drafts ready to review below.")
        st.rerun()

    st.divider()

    # ── Draft review table ───────────────────────────────────────────────────

    all_drafts = get_all_drafts()

    if not all_drafts:
        st.info("No drafts yet. Click 'Generate Drafts' above.")
    else:
        drf1, drf2 = st.columns([1, 3])
        with drf1:
            draft_status_filter = st.selectbox(
                "Draft status",
                options=["All", "pending", "sent"],
                label_visibility="collapsed",
                key="draft_status_filter",
            )
        with drf2:
            draft_search = st.text_input(
                "search drafts",
                placeholder="Filter by name or business...",
                label_visibility="collapsed",
                key="draft_search",
            )

        filtered_drafts = all_drafts
        if draft_status_filter != "All":
            filtered_drafts = [d for d in filtered_drafts if d["status"] == draft_status_filter]
        if draft_search.strip():
            term = draft_search.strip().lower()
            filtered_drafts = [
                d for d in filtered_drafts
                if term in (d.get("person_name") or "").lower()
                or term in (d.get("business_name") or "").lower()
            ]

        st.caption(f"Showing **{len(filtered_drafts)}** of **{len(all_drafts)}** drafts")

        # Emails already sent — persists across draft clears via campaigns table
        # get_campaign_status_map returns {email: sent_date} for all sent rows
        sent_emails = set(get_campaign_status_map().keys())

        for draft in filtered_drafts:
            candidate_emails = draft.get("candidate_emails") or []
            all_sent = draft["status"] == "sent" or (
                bool(candidate_emails) and all(e.lower() in sent_emails for e in candidate_emails)
            )
            status_label = "SENT" if all_sent else draft["status"].upper()

            with st.expander(
                f"{draft['person_name']} — {draft['title']} | {draft['business_name']} | [{status_label}]"
            ):
                if draft.get("website_url"):
                    st.markdown(f"**Website:** [{draft['website_url']}]({draft['website_url']})")
                st.markdown(f"**To:** {', '.join(candidate_emails) or '—'}")
                st.markdown(f"**Subject:** {draft['subject']}")
                st.markdown("**Body:**")
                st.text(draft["body"])

                if all_sent:
                    st.caption("Already sent to all addresses for this person.")
                else:
                    btn_col1, btn_col2 = st.columns([2, 1])
                    with btn_col1:
                        send_clicked = st.button(
                            "Send to all addresses",
                            key=f"send_draft_{draft['id']}",
                            type="primary",
                            use_container_width=True,
                        )
                    with btn_col2:
                        delete_clicked = st.button(
                            "Delete",
                            key=f"delete_draft_{draft['id']}",
                            use_container_width=True,
                        )
                    if delete_clicked:
                        delete_draft(draft["id"])
                        st.rerun()
                    if send_clicked:
                        any_ok = False
                        for email in candidate_emails:
                            if email.lower() in sent_emails:
                                continue
                            ok, msg = send_email(
                                to_address=email,
                                subject=draft["subject"],
                                body=draft["body"],
                            )
                            if ok:
                                save_campaign_send(email, draft["business_name"], "sent")
                                sent_emails.add(email.lower())
                                any_ok = True
                            else:
                                st.warning(f"Failed to send to {email}: {msg}")
                        if any_ok:
                            mark_draft_sent(draft["id"])
                            st.success(f"Sent to all addresses for {draft['person_name']}")
                            st.rerun()
                        else:
                            st.error("All sends failed — check SMTP settings.")
