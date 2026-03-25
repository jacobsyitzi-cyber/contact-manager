import io
import zipfile
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from rcuk_contact_manager.core import (
    df_to_contacts,
    export_vcf,
    find_duplicates,
    load_spreadsheet,
    merge_contacts,
    parse_file,
    parse_vcf,
)

APP_NAME = "YJ - RCUK CONTACT MANAGER"

st.set_page_config(page_title=APP_NAME, page_icon="📇", layout="wide", initial_sidebar_state="collapsed")


def inject_css() -> None:
    st.markdown(
        """
        <style>
            .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1500px;}
            .app-hero {background: linear-gradient(135deg, #0f172a 0%, #111827 45%, #1e293b 100%); border: 1px solid rgba(255,255,255,0.06); border-radius: 22px; padding: 24px 26px 20px 26px; margin-bottom: 18px; color: white; box-shadow: 0 10px 30px rgba(0,0,0,0.15);}
            .hero-title {font-size: 1.75rem; font-weight: 700; margin: 0; line-height: 1.2;}
            .hero-sub {margin-top: 8px; color: rgba(255,255,255,0.78); font-size: 0.98rem;}
            .section-card {background: rgba(255,255,255,0.02); border: 1px solid rgba(128,128,128,0.18); border-radius: 18px; padding: 16px 16px 12px 16px; margin-bottom: 14px;}
            .group-card {border: 1px solid rgba(128,128,128,0.18); border-radius: 18px; padding: 14px; background: rgba(255,255,255,0.015); margin-bottom: 12px;}
            .contact-card {border: 1px solid rgba(128,128,128,0.16); border-radius: 16px; padding: 14px; background: rgba(255,255,255,0.02); min-height: 210px;}
            .contact-title {font-weight: 700; font-size: 1rem; margin-bottom: 8px;}
            .muted {color: #94a3b8; font-size: 0.92rem;}
            .footer-note {opacity: 0.75; font-size: 0.85rem; padding-top: 8px;}
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_css()


def init_state() -> None:
    defaults = {
        "convert_contacts": [],
        "merge_contacts": [],
        "dedupe_input_contacts": [],
        "dedupe_groups": [],
        "dedupe_result_contacts": [],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


def contacts_to_df(contacts: List[Any], limit: Optional[int] = None) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for c in contacts[: limit or len(contacts)]:
        if isinstance(c, str):
            rows.append(parse_vcard_summary(c))
        elif isinstance(c, dict):
            rows.append(c)
        else:
            rows.append({"value": str(c)})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["No contacts"])


def parse_vcard_summary(card: str) -> Dict[str, str]:
    name = ""
    phones: List[str] = []
    emails: List[str] = []
    for line in card.replace("\r\n", "\n").split("\n"):
        if line.startswith("FN:"):
            name = line[3:].strip()
        elif line.startswith("TEL") and ":" in line:
            phones.append(line.split(":", 1)[1].strip())
        elif line.startswith("EMAIL") and ":" in line:
            emails.append(line.split(":", 1)[1].strip())
    return {"name": name, "phones": ", ".join(phones), "emails": ", ".join(emails)}


def get_contact_name(card: str) -> str:
    return parse_vcard_summary(card).get("name") or "Unnamed contact"


def build_zip(vcf_filename: str, vcf_bytes: bytes) -> bytes:
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(vcf_filename, vcf_bytes)
    memory_file.seek(0)
    return memory_file.read()


def top_header() -> None:
    st.markdown(
        f"""
        <div class="app-hero">
            <div class="hero-title">📇 {APP_NAME}</div>
            <div class="hero-sub">Convert, merge and de-duplicate contacts in Streamlit.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def convert_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Convert to VCF")
    st.caption("Upload CSV, XLSX, VCF, NBF, NBU, ZIP or IB files.")
    st.markdown("</div>", unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Drag and drop a contact file here",
        type=["csv", "xlsx", "vcf", "nbf", "nbu", "zip", "ib"],
        accept_multiple_files=False,
        key="convert_upload_file",
    )

    preview_limit = st.selectbox("Preview rows", [25, 50, 100, 250, 500], index=3, key="convert_preview_limit")
    export_zip = st.checkbox("Download as ZIP", value=False, key="convert_export_zip")

    if not uploaded:
        st.info("Upload a file to begin.")
        return

    st.success(f"Loaded: {uploaded.name}")
    lower = uploaded.name.lower()

    if lower.endswith((".csv", ".xlsx")):
        try:
            df = load_spreadsheet(uploaded)
        except Exception as exc:
            st.error(str(exc))
            return
        if df.empty:
            st.error("The spreadsheet appears empty.")
            return
        st.dataframe(df.head(preview_limit), use_container_width=True)
        cols = list(df.columns)
        c1, c2, c3 = st.columns(3)
        with c1:
            name_col = st.selectbox("Name column", cols, key="convert_name_column")
        with c2:
            phone_col = st.selectbox("Phone column", cols, key="convert_phone_column")
        with c3:
            email_col = st.selectbox("Email column", cols, key="convert_email_column")
        if st.button("Convert now", key="convert_run_button", use_container_width=True):
            try:
                st.session_state["convert_contacts"] = df_to_contacts(df, name_col, phone_col, email_col)
                st.success(f"Converted {len(st.session_state['convert_contacts']):,} contacts.")
            except Exception as exc:
                st.error(f"Conversion failed: {exc}")
    else:
        if st.button("Read file", key="convert_read_general_button", use_container_width=True):
            try:
                st.session_state["convert_contacts"] = parse_file(uploaded)
                st.success(f"Imported {len(st.session_state['convert_contacts']):,} contacts.")
            except Exception as exc:
                st.error(str(exc))

    contacts = st.session_state["convert_contacts"]
    if not contacts:
        return

    st.dataframe(contacts_to_df(contacts, preview_limit), use_container_width=True)
    vcf_bytes = export_vcf(contacts)
    st.download_button("Download VCF", data=vcf_bytes, file_name="contacts.vcf", mime="text/vcard", key="convert_download_vcf_button", use_container_width=True)
    if export_zip:
        st.download_button("Download ZIP", data=build_zip("contacts.vcf", vcf_bytes), file_name="contacts.zip", mime="application/zip", key="convert_download_zip_button", use_container_width=True)


def merge_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Merge VCFs")
    st.caption("Upload multiple VCF files and export one combined file.")
    st.markdown("</div>", unsafe_allow_html=True)

    files = st.file_uploader("Drag and drop one or more VCF files here", type=["vcf"], accept_multiple_files=True, key="merge_upload_files")
    merge_dedupe = st.checkbox("Remove exact duplicate groups", value=False, key="merge_remove_duplicates_checkbox")
    merge_smart_merge = st.checkbox("Smart merge duplicates", value=False, key="merge_smart_merge_checkbox")
    preview_limit = st.selectbox("Preview rows", [25, 50, 100, 250, 500], index=3, key="merge_preview_limit")

    if not files:
        st.info("Upload one or more VCF files to begin.")
        return

    st.write(f"Loaded {len(files)} file(s).")
    if st.button("Merge files", key="merge_run_button", use_container_width=True):
        all_contacts: List[str] = []
        for file in files:
            all_contacts.extend(parse_vcf(file))
        merged_contacts = all_contacts
        if merge_dedupe or merge_smart_merge:
            groups = find_duplicates(merged_contacts)
            used = set()
            final: List[str] = []
            for group in groups:
                if merge_smart_merge:
                    final.append(merge_contacts(group))
                else:
                    final.append(group[0])
                used.update(group)
            for card in merged_contacts:
                if card not in used:
                    final.append(card)
            merged_contacts = final
        st.session_state["merge_contacts"] = merged_contacts
        st.success(f"Prepared {len(merged_contacts):,} contacts.")

    contacts = st.session_state["merge_contacts"]
    if not contacts:
        return

    st.dataframe(contacts_to_df(contacts, preview_limit), use_container_width=True)
    st.download_button("Download merged VCF", data=export_vcf(contacts), file_name="merged_contacts.vcf", mime="text/vcard", key="merge_download_button", use_container_width=True)


def render_duplicate_group(group_index: int, group: List[str]) -> None:
    st.markdown('<div class="group-card">', unsafe_allow_html=True)
    st.markdown(f"### Duplicate group {group_index + 1}")
    labels = [f"{idx + 1}. {get_contact_name(card)}" for idx, card in enumerate(group)]
    keep_choice = st.radio("Choose which contact to keep", options=list(range(len(group))), format_func=lambda x: labels[x], key=f"dedupe_keep_choice_{group_index}")
    cols = st.columns(len(group))
    for j, contact in enumerate(group):
        with cols[j]:
            summary = parse_vcard_summary(contact)
            st.markdown('<div class="contact-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="contact-title">{summary.get("name") or "Unnamed contact"}</div>', unsafe_allow_html=True)
            st.write(summary.get("phones") or "No phone numbers")
            st.write(summary.get("emails") or "No email addresses")
            st.markdown("</div>", unsafe_allow_html=True)
            if j == keep_choice:
                st.success("Selected to keep")
    st.markdown("</div>", unsafe_allow_html=True)


def dedupe_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("VCF De-duplicate")
    st.caption("Upload a VCF, review duplicate groups, then export a cleaned file.")
    st.markdown("</div>", unsafe_allow_html=True)

    uploaded = st.file_uploader("Drag and drop a VCF file here", type=["vcf"], accept_multiple_files=False, key="dedupe_upload_file")
    match_by_email = st.checkbox("Match by email", value=True, key="dedupe_match_by_email_checkbox")
    match_by_phone = st.checkbox("Match by phone", value=True, key="dedupe_match_by_phone_checkbox")
    match_by_name = st.checkbox("Match by name", value=True, key="dedupe_match_by_name_checkbox")
    smart_merge = st.checkbox("Smart merge duplicates", value=False, key="dedupe_smart_merge_checkbox")
    preview_limit = st.selectbox("Preview rows", [25, 50, 100, 250, 500], index=3, key="dedupe_preview_limit")

    if not uploaded:
        st.info("Upload a VCF file to begin.")
        return

    if st.button("Load VCF", key="dedupe_load_button", use_container_width=True):
        st.session_state["dedupe_input_contacts"] = parse_vcf(uploaded)
        st.session_state["dedupe_groups"] = []
        st.session_state["dedupe_result_contacts"] = []
        st.success(f"Loaded {len(st.session_state['dedupe_input_contacts']):,} contacts.")

    input_contacts = st.session_state["dedupe_input_contacts"]
    if not input_contacts:
        return

    st.dataframe(contacts_to_df(input_contacts, preview_limit), use_container_width=True)

    if st.button("Find duplicates", key="dedupe_scan_button", use_container_width=True):
        st.session_state["dedupe_groups"] = find_duplicates(input_contacts, match_by_email, match_by_phone, match_by_name)
        st.session_state["dedupe_groups"] = [g for g in st.session_state["dedupe_groups"] if len(g) > 1]
        st.success(f"Found {len(st.session_state['dedupe_groups']):,} duplicate group(s).")

    dup_groups = st.session_state["dedupe_groups"]
    if dup_groups:
        for idx, group in enumerate(dup_groups):
            render_duplicate_group(idx, group)
        if st.button("Apply choices", key="dedupe_apply_choices_button", use_container_width=True):
            result_contacts: List[str] = []
            used = set()
            for idx, group in enumerate(dup_groups):
                if smart_merge:
                    result_contacts.append(merge_contacts(group))
                else:
                    keep_idx = st.session_state.get(f"dedupe_keep_choice_{idx}", 0)
                    result_contacts.append(group[keep_idx])
                used.update(group)
            for card in input_contacts:
                if card not in used:
                    result_contacts.append(card)
            st.session_state["dedupe_result_contacts"] = result_contacts
            st.success(f"Prepared {len(result_contacts):,} cleaned contacts.")

    result_contacts = st.session_state["dedupe_result_contacts"]
    if not result_contacts:
        return

    st.dataframe(contacts_to_df(result_contacts, preview_limit), use_container_width=True)
    st.download_button("Download cleaned VCF", data=export_vcf(result_contacts), file_name="deduplicated_contacts.vcf", mime="text/vcard", key="dedupe_download_button", use_container_width=True)


def help_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Help")
    st.markdown(
        """
        **Convert to VCF**
        - Upload a supported file
        - For spreadsheets, map Name / Phone / Email
        - Convert and download the result

        **Merge VCFs**
        - Upload multiple `.vcf` files
        - Merge them into one export
        - Optionally remove or smart-merge duplicates

        **VCF De-duplicate**
        - Upload one VCF
        - Find duplicate groups
        - Review each group and choose what to keep
        - Export a cleaned VCF
        """
    )
    st.markdown("</div>", unsafe_allow_html=True)


def main() -> None:
    top_header()
    tab_convert, tab_merge, tab_dedupe, tab_help = st.tabs(["Convert to VCF", "Merge VCFs", "VCF De-duplicate", "Help"])
    with tab_convert:
        convert_tab()
    with tab_merge:
        merge_tab()
    with tab_dedupe:
        dedupe_tab()
    with tab_help:
        help_tab()
    st.markdown('<div class="footer-note">ROSE COMMUNICATIONS GROUP LTD • Streamlit rebuild</div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()
