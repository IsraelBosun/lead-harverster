"""
LeadHarvest — Streamlit Frontend

Connects to the FastAPI backend at http://127.0.0.1:8000 and provides a
browser UI for triggering scrape jobs, monitoring progress, viewing results,
and downloading the Excel export.

Prerequisites:
    Start the FastAPI backend first:
        venv/Scripts/python.exe run_api.py

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
    get_contact_stats,
    get_unenriched_count,
    get_verification_stats,
    get_sendable_contacts,
    get_verified_contacts_without_drafts,
    get_campaign_status_map,
    get_opened_leads,
    save_campaign_send,
    get_all_drafts,
    get_draft_stats,
    mark_draft_sent,
    delete_draft,
    update_draft,
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

tab_scrape, tab_enrich, tab_outreach = st.tabs(["Scrape", "Enrich & Verify", "Outreach"])




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
        run_clicked = st.button("Run Scrape", type="primary", width='stretch')

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
                    width='stretch',
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

        if st.button("Start a New Scrape", type="primary", width='stretch'):
            _reset_to_idle()
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Enrich & Verify
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Enrich & Verify
# ══════════════════════════════════════════════════════════════════════════════

with tab_enrich:

    st.markdown('<p class="lh-section-title">Enrich & Verify</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="lh-section-sub">'
        'Find decision-makers on business websites, then verify their emails via mails.so.</p>',
        unsafe_allow_html=True,
    )

    # ── Stats ────────────────────────────────────────────────────────────────

    dm_stats       = get_contact_stats()
    ver_stats      = get_verification_stats()
    pending_enrich = get_unenriched_count(country="Nigeria")

    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Enriched",             dm_stats["businesses_enriched"])
    e2.metric("Contacts Found",       dm_stats["total_contacts"])
    e3.metric("Pending Enrich",       pending_enrich)
    e4.metric("Verified / Catch-All", f"{ver_stats['verified']} / {ver_stats['catch_all']}")

    st.divider()

    # ── Find Decision Makers ─────────────────────────────────────────────────

    st.markdown("**Find Decision Makers**")
    st.caption(
        "Visits team/about pages of unenriched businesses, uses Gemini to extract "
        "names and titles. Sites that time out are skipped and retried next run."
    )

    if not os.getenv("GEMINI_API_KEY"):
        st.warning("GEMINI_API_KEY not set in .env — enrichment will return no results.")

    enrich_col1, enrich_col2 = st.columns([3, 1])
    with enrich_col1:
        enrich_limit = st.number_input(
            "Businesses to enrich",
            min_value=1,
            max_value=max(pending_enrich, 1),
            value=min(10, max(pending_enrich, 1)),
            step=10,
            help="Each business takes 15-60 seconds depending on site speed.",
        )
    with enrich_col2:
        st.markdown("<div style='height:1.85rem'></div>", unsafe_allow_html=True)
        enrich_clicked = st.button("Run Enrichment", type="primary", width='stretch')

    if enrich_clicked:
        from enricher.enricher import enrich_businesses
        with st.spinner(f"Enriching up to {enrich_limit} businesses — this may take several minutes..."):
            result = enrich_businesses(limit=int(enrich_limit))
        ec1, ec2, ec3 = st.columns(3)
        ec1.metric("Processed",      result["businesses_processed"])
        ec2.metric("People Found",   result["people_found"])
        ec3.metric("Contacts Saved", result["saved"])
        if result["saved"] > 0:
            st.success(f"{result['saved']} new contacts saved.")
        elif result["people_found"] == 0:
            st.info("No decision-maker profiles found. Try running again for more businesses.")
        st.rerun()

    st.divider()

    # ── Verify Emails ────────────────────────────────────────────────────────

    st.markdown("**Verify Contact Emails**")
    st.caption(
        "Generates all email pattern variants per person and pings each via mails.so. "
        "Domains that time out are skipped. Results saved to the verification table."
    )

    if not os.getenv("MAILS_SO_API_KEY"):
        st.warning("MAILS_SO_API_KEY not set in .env — verification will not run.")

    v1, v2 = st.columns([3, 1])
    with v2:
        verify_clicked = st.button("Run Verification", type="primary", width='stretch')
    with v1:
        new_count = ver_stats["new_persons_to_verify"]
        if new_count == 0:
            st.caption("No new contacts to verify — all enriched domains have already been processed.")
        else:
            st.caption(f"{new_count} newly enriched person{'s' if new_count != 1 else ''} ready to verify.")

    if verify_clicked:
        from verify_contacts import run_verification
        with st.spinner("Verifying contact emails via mails.so — this may take a few minutes..."):
            vresult = run_verification()
        if vresult is None or (vresult.get("verified", 0) == 0 and vresult.get("catch_all", 0) == 0 and vresult.get("rejected", 0) == 0):
            st.rerun()
        else:
            vc1, vc2, vc3, vc4 = st.columns(4)
            vc1.metric("Verified",  vresult.get("verified", 0))
            vc2.metric("Catch-All", vresult.get("catch_all", 0))
            vc3.metric("Rejected",  vresult.get("rejected", 0))
            vc4.metric("Unknown",   vresult.get("unknown", 0))
            st.success(f"Done. {vresult.get('total_saved', 0)} total results saved.")
            st.rerun()

    st.divider()

    # ── Results Table ────────────────────────────────────────────────────────

    sendable = get_sendable_contacts()
    st.markdown(
        f'<p class="lh-section-title">'
        f'Ready to Send <span style="font-weight:400;color:#9ca3af;">'
        f'{len(sendable)} contacts</span></p>',
        unsafe_allow_html=True,
    )

    if not sendable:
        st.info("No verified or catch-all contacts yet. Run enrichment then verification above.")
    else:
        import pandas as pd
        sent_map = set(get_campaign_status_map().keys())
        df_sendable = pd.DataFrame(sendable)[["status", "email", "person_name", "title", "business_name", "domain", "ssl_issue"]]
        df_sendable.columns = ["Status", "Email", "Name", "Title", "Business", "Domain", "ssl_issue"]
        df_sendable["_ord"] = df_sendable["Status"].map({"verified": 0, "catch_all": 1}).fillna(2)
        df_sendable = df_sendable.sort_values("_ord").drop(columns="_ord").reset_index(drop=True)
        df_sendable["Sent"] = df_sendable["Email"].str.lower().apply(lambda e: "Yes" if e in sent_map else "No")
        df_sendable["SSL Issue"] = df_sendable["ssl_issue"].apply(lambda x: "Yes" if x else "")
        df_sendable = df_sendable.drop(columns="ssl_issue")
        df_sendable.insert(0, "#", df_sendable.index + 1)
        st.dataframe(
            df_sendable,
            width='stretch',
            hide_index=True,
            column_config={"#": st.column_config.NumberColumn("#", width="small")},
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Outreach
# ══════════════════════════════════════════════════════════════════════════════

with tab_outreach:

    _EMAIL_DAILY_LIMIT = int(os.getenv("EMAIL_DAILY_LIMIT", "50"))
    _EMAIL_DELAY       = float(os.getenv("EMAIL_DELAY_SECONDS", "3"))

    st.markdown('<p class="lh-section-title">Outreach</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="lh-section-sub">'
        'Generate personalised Gemini drafts for verified contacts and send cold emails.</p>',
        unsafe_allow_html=True,
    )

    # ── Stats ────────────────────────────────────────────────────────────────

    ver_stats_out   = get_verification_stats()
    draft_stats_out = get_draft_stats()
    remaining_today = max(0, _EMAIL_DAILY_LIMIT - ver_stats_out["sent_today"])

    o1, o2, o3, o4, o5 = st.columns(5)
    o1.metric("Ready to Send",   ver_stats_out["not_emailed"])
    o2.metric("Sent Total",      ver_stats_out["sent_total"])
    o3.metric("Sent Today",      ver_stats_out["sent_today"])
    o4.metric("Remaining Today", remaining_today)
    o5.metric("Drafts Pending",  draft_stats_out["pending"])

    st.divider()

    # ── Verified Contacts Table ───────────────────────────────────────────────

    verified_sendable = [c for c in get_sendable_contacts() if c["status"] == "verified"]
    st.markdown(
        f'<p class="lh-section-title">'
        f'Verified Contacts <span style="font-weight:400;color:#9ca3af;">'
        f'{len(verified_sendable)} contacts</span></p>',
        unsafe_allow_html=True,
    )

    if not verified_sendable:
        st.info("No verified contacts yet. Run enrichment and verification in the Enrich tab.")
    else:
        import pandas as pd
        sent_map_out = set(get_campaign_status_map().keys())
        df_verified = pd.DataFrame(verified_sendable)[["email", "person_name", "title", "business_name", "domain", "ssl_issue"]]
        df_verified.columns = ["Email", "Name", "Title", "Business", "Domain", "ssl_issue"]
        df_verified["Sent"] = df_verified["Email"].str.lower().apply(lambda e: "Yes" if e in sent_map_out else "No")
        df_verified["SSL Issue"] = df_verified["ssl_issue"].apply(lambda x: "Yes" if x else "")
        df_verified = df_verified.drop(columns="ssl_issue")
        df_verified.insert(0, "#", df_verified.index + 1)
        st.dataframe(
            df_verified,
            width='stretch',
            hide_index=True,
            column_config={"#": st.column_config.NumberColumn("#", width="small")},
        )

    st.divider()

    # ── Test Delivery ─────────────────────────────────────────────────────────

    st.markdown("**Test Delivery**")
    t1, t2 = st.columns([3, 1])
    with t1:
        test_addr = st.text_input(
            "Test email address",
            placeholder="yourname@gmail.com",
            label_visibility="collapsed",
        )
    with t2:
        test_clicked = st.button("Send Test", type="secondary", width='stretch')

    if test_clicked:
        if not test_addr.strip() or "@" not in test_addr:
            st.warning("Enter a valid email address.")
        else:
            from enricher.gemini_extractor import draft_email_with_gemini
            sample_page = (
                "Okafor & Associates is a full-service Nigerian law firm based in Lagos. "
                "We specialise in corporate law, mergers and acquisitions, litigation, and real estate. "
                "Our team of senior partners has over 20 years of combined experience advising "
                "multinationals and high-net-worth individuals across West Africa. "
                "We handle client matters manually through email and phone, and have no online client portal."
            )
            with st.spinner("Composing test email with Gemini..."):
                draft = draft_email_with_gemini(
                    person_name="Chidi Okafor",
                    title="Managing Partner",
                    business_name="Okafor & Associates",
                    page_text=sample_page,
                )
            if draft:
                subject, body = draft["subject"], draft["body"]
            else:
                from emailer.templates import render as render_template
                subject, body = render_template("Okafor & Associates")

            with st.spinner("Sending test..."):
                ok, reason = send_email(test_addr.strip(), subject, body)
            if ok:
                st.success(f"Test sent to {test_addr}. Check your inbox.")
            else:
                st.error(f"Send failed: {reason}")

    st.divider()

    # ── Generate Drafts ───────────────────────────────────────────────────────

    st.markdown("**Generate Drafts**")
    st.caption("Gemini writes a personalised cold email for each verified contact only. Catch-all and other statuses are excluded.")

    gc1, gc2 = st.columns([3, 1])
    with gc1:
        draft_limit = st.number_input(
            "Contacts to draft for",
            min_value=1, max_value=500, value=20, step=10,
            label_visibility="collapsed",
            help="Each draft takes ~2 seconds via Gemini.",
        )
    with gc2:
        draft_clicked = st.button("Generate Drafts", type="primary", width='stretch')

    if draft_clicked:
        import enricher.drafter as _drafter
        _orig_fn = _drafter.get_contacts_without_drafts
        _drafter.get_contacts_without_drafts = lambda: get_verified_contacts_without_drafts(limit=500)
        from enricher.drafter import generate_drafts
        with st.spinner(f"Generating up to {draft_limit} drafts with Gemini..."):
            dresult = generate_drafts(limit=int(draft_limit))
        _drafter.get_contacts_without_drafts = _orig_fn
        dc1, dc2, dc3 = st.columns(3)
        dc1.metric("Processed", dresult["contacts_processed"])
        dc2.metric("Saved",     dresult["drafts_saved"])
        dc3.metric("Skipped",   dresult["skipped"])
        if dresult["drafts_saved"] > 0:
            st.success(f"{dresult['drafts_saved']} drafts ready below.")
        st.rerun()

    st.divider()

    # ── Draft Cards ───────────────────────────────────────────────────────────

    all_drafts  = get_all_drafts()
    sent_emails = set(get_campaign_status_map().keys())

    pending_drafts = [
        d for d in all_drafts
        if d["status"] != "sent"
        and not all(e.lower() in sent_emails for e in (d.get("candidate_emails") or []))
    ]

    if not pending_drafts:
        if all_drafts:
            st.info("All drafts have been sent.")
        else:
            st.info("No drafts yet. Click Generate Drafts above.")
    else:
        sa1, sa2 = st.columns([4, 1])
        with sa1:
            st.caption(f"{len(pending_drafts)} drafts pending.")
        with sa2:
            send_all_clicked = st.button(
                "Send All", type="primary", width='stretch', key="send_all_drafts"
            )

        if send_all_clicked:
            total_ok = total_fail = 0
            with st.spinner("Sending all pending drafts..."):
                for draft in pending_drafts:
                    for email in (draft.get("candidate_emails") or []):
                        if email.lower() in sent_emails:
                            continue
                        ok, _ = send_email(email, draft["subject"], draft["body"])
                        if ok:
                            save_campaign_send(email, draft["business_name"], "sent")
                            sent_emails.add(email.lower())
                            total_ok += 1
                            mark_draft_sent(draft["id"])
                        else:
                            total_fail += 1
            if total_ok:
                st.success(f"Sent {total_ok} emails.")
            if total_fail:
                st.warning(f"{total_fail} failed — check SMTP settings.")
            st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)

        for draft in pending_drafts:
            candidate_emails = draft.get("candidate_emails") or []
            with st.expander(
                f"{draft['person_name']} — {draft['title']} | {draft['business_name']}"
            ):
                st.markdown(f"**To:** {', '.join(candidate_emails) or 'No email'}")
                if draft.get("website_url"):
                    st.markdown(f"**Website:** [{draft['website_url']}]({draft['website_url']})")
                edited_subject = st.text_input(
                    "Subject", value=draft["subject"], key=f"subj_{draft['id']}"
                )
                edited_body = st.text_area(
                    "Body", value=draft["body"], key=f"body_{draft['id']}", height=200
                )
                btn1, btn2 = st.columns(2)
                with btn1:
                    if st.button("Send", key=f"send_{draft['id']}", type="primary", width='stretch'):
                        update_draft(draft["id"], edited_subject, edited_body)
                        any_ok = False
                        for email in candidate_emails:
                            if email.lower() in sent_emails:
                                continue
                            ok, msg = send_email(email, edited_subject, edited_body)
                            if ok:
                                save_campaign_send(email, draft["business_name"], "sent")
                                sent_emails.add(email.lower())
                                any_ok = True
                            else:
                                st.warning(f"Failed: {email} — {msg}")
                        if any_ok:
                            mark_draft_sent(draft["id"])
                            st.success(f"Sent to {draft['person_name']}")
                            st.rerun()
                        else:
                            st.error("Send failed — check SMTP settings.")
                with btn2:
                    if st.button("Delete", key=f"del_{draft['id']}", width='stretch'):
                        delete_draft(draft["id"])
                        st.rerun()

    st.divider()

    # ── Sent Log ─────────────────────────────────────────────────────────────

    st.markdown("**Sent Log**")
    opened = get_opened_leads()
    if opened:
        import pandas as pd
        df_log = pd.DataFrame(opened)
        df_log.columns = ["Email", "Business", "Sent At", "Opened At"]
        st.dataframe(df_log, width='stretch', hide_index=True)
    else:
        st.caption("No sends logged yet.")
