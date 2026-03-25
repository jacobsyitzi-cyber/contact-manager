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


# ============================================================
# Page setup
# ============================================================
st.set_page_config(
    page_title=APP_NAME,
    page_icon="📇",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================
# Styling
# ============================================================
def inject_css() -> None:
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.2rem;
                padding-bottom: 2rem;
                max-width: 1500px;
            }

            .app-hero {
                background: linear-gradient(135deg, #0f172a 0%, #111827 45%, #1e293b 100%);
                border: 1px solid rgba(255,255,255,0.06);
                border-radius: 22px;
                padding: 24px 26px 20px 26px;
                margin-bottom: 18px;
                color: white;
                box-shadow: 0 10px 30px rgba(0,0,0,0.15);
            }

            .hero-title {
                font-size: 1.75rem;
                font-weight: 700;
                margin: 0;
                line-height: 1.2;
            }

            .hero-sub {
                margin-top: 8px;
                color: rgba(255,255,255,0.78);
                font-size: 0.98rem;
            }

            .section-card {
                background: rgba(255,255,255,0.02);
                border: 1px solid rgba(128,128,128,0.18);
                border-radius: 18px;
                padding: 16px 16px 12px 16px;
                margin-bottom: 14px;
            }

            .mini-stat {
                background: rgba(255,255,255,0.02);
                border: 1px solid rgba(128,128,128,0.18);
                border-radius: 18px;
                padding: 14px 16px;
            }

            .group-card {
                border: 1px solid rgba(128,128,128,0.18);
                border-radius: 18px;
                padding: 14px;
                background: rgba(255,255,255,0.015);
                margin-bottom: 12px;
            }

            .contact-card {
                border: 1px solid rgba(128,128,128,0.16);
                border-radius: 16px;
                padding: 14px;
                background: rgba(255,255,255,0.02);
                min-height: 210px;
            }

            .contact-title {
                font-weight: 700;
                font-size: 1rem;
                margin-bottom: 8px;
            }

            .muted {
                color: #94a3b8;
                font-size: 0.92rem;
            }

            .footer-note {
                opacity: 0.75;
                font-size: 0.85rem;
                padding-top: 8px;
            }

            div[data-testid="stMetric"] {
                border: 1px solid rgba(128,128,128,0.14);
                border-radius: 16px;
                padding: 10px 8px;
                background: rgba(255,255,255,0.015);
            }

            div[data-testid="stDataFrame"] {
                border-radius: 14px;
                overflow: hidden;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_css()


# ============================================================
# Session state
# ============================================================
def init_state() -> None:
    defaults = {
        "convert_contacts": [],
        "merge_contacts": [],
        "dedupe_input_contacts": [],
        "dedupe_groups": [],
        "dedupe_keep_map": {},
        "dedupe_result_contacts": [],
        "convert_preview_limit": 250,
        "merge_preview_limit": 250,
        "dedupe_preview_limit": 250,
        "last_error": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()


# ============================================================
# Helpers
# ============================================================
def safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_contact_for_table(contact: Any) -> Dict[str, Any]:
    if isinstance(contact, dict):
        return contact

    return {"raw": str(contact)}


def contacts_to_df(contacts: List[Any], limit: Optional[int] = None) -> pd.DataFrame:
    rows = [normalize_contact_for_table(c) for c in contacts]
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        return pd.DataFrame(columns=["No contacts"])
    return pd.DataFrame(rows)


def get_contact_display_name(contact: Any) -> str:
    if isinstance(contact, dict):
        for key in ["full_name", "name", "display_name", "fn", "Name", "Full Name"]:
            value = contact.get(key)
            if value:
                return str(value)
    return "Unnamed contact"


def contact_value_list(contact: Any, possible_keys: List[str]) -> List[str]:
    if not isinstance(contact, dict):
        return [str(contact)]

    out: List[str] = []
    for key in possible_keys:
        if key not in contact:
            continue
        value = contact.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, list):
            out.extend([str(v) for v in value if v not in (None, "")])
        else:
            out.append(str(value))

    seen = set()
    unique = []
    for item in out:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def render_contact_card(contact: Any, prefix: str) -> None:
    name = get_contact_display_name(contact)
    phones = contact_value_list(
        contact,
        ["phones", "phone", "mobile", "Phone", "Telephone", "tel"],
    )
    emails = contact_value_list(
        contact,
        ["emails", "email", "Email", "mail"],
    )
    orgs = contact_value_list(
        contact,
        ["org", "company", "organisation", "organization", "Company"],
    )

    st.markdown('<div class="contact-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="contact-title">{name}</div>', unsafe_allow_html=True)

    if phones:
        st.markdown("**Phones**")
        for item in phones[:6]:
            st.write(item)
    else:
        st.markdown('<div class="muted">No phone numbers</div>', unsafe_allow_html=True)

    st.markdown("---")

    if emails:
        st.markdown("**Emails**")
        for item in emails[:6]:
            st.write(item)
    else:
        st.markdown('<div class="muted">No email addresses</div>', unsafe_allow_html=True)

    if orgs:
        st.markdown("---")
        st.markdown("**Company / Organisation**")
        for item in orgs[:3]:
            st.write(item)

    st.markdown("</div>", unsafe_allow_html=True)


def build_vcf_download_data(contacts: List[Any]) -> bytes:
    content = export_vcf(contacts)
    if isinstance(content, bytes):
        return content
    return str(content).encode("utf-8")


def build_zip_of_vcf(vcf_filename: str, vcf_bytes: bytes) -> bytes:
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(vcf_filename, vcf_bytes)
    memory_file.seek(0)
    return memory_file.read()


def show_error(message: str) -> None:
    st.session_state["last_error"] = message
    st.error(message)


def show_success(message: str) -> None:
    st.success(message)


def try_parse_general_file(uploaded_file) -> List[Any]:
    try:
        contacts = parse_file(uploaded_file)
        return safe_list(contacts)
    except Exception as exc:
        raise RuntimeError(f"Could not read file '{uploaded_file.name}': {exc}") from exc


def try_parse_vcf_file(uploaded_file) -> List[Any]:
    try:
        contacts = parse_vcf(uploaded_file)
        return safe_list(contacts)
    except Exception as exc:
        raise RuntimeError(f"Could not read VCF '{uploaded_file.name}': {exc}") from exc


def try_load_spreadsheet(uploaded_file) -> pd.DataFrame:
    try:
        df = load_spreadsheet(uploaded_file)
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        return df
    except Exception as exc:
        raise RuntimeError(f"Could not load spreadsheet '{uploaded_file.name}': {exc}") from exc


def top_header() -> None:
    st.markdown(
        f"""
        <div class="app-hero">
            <div class="hero-title">📇 {APP_NAME}</div>
            <div class="hero-sub">
                Convert, merge and de-duplicate contacts in a cleaner Streamlit interface.
                Built for desktop use with drag-and-drop uploads and browser downloads.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stat_row(convert_count: int, merge_count: int, dedupe_count: int, dup_groups: int) -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Convert contacts", f"{convert_count:,}")
    with c2:
        st.metric("Merge contacts", f"{merge_count:,}")
    with c3:
        st.metric("Dedupe contacts", f"{dedupe_count:,}")
    with c4:
        st.metric("Duplicate groups", f"{dup_groups:,}")


# ============================================================
# Convert tab
# ============================================================
def convert_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Convert to VCF")
    st.caption(
        "Upload CSV, XLSX, VCF, NBF, NBU, ZIP or other supported files. "
        "Spreadsheet imports let you map columns before conversion."
    )
    st.markdown("</div>", unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Drag and drop a contact file here",
        type=["csv", "xlsx", "vcf", "nbf", "nbu", "zip"],
        accept_multiple_files=False,
        key="convert_upload_file",
    )

    convert_toolbar_col1, convert_toolbar_col2, convert_toolbar_col3 = st.columns([1, 1, 2])

    with convert_toolbar_col1:
        preview_limit = st.selectbox(
            "Preview rows",
            options=[25, 50, 100, 250, 500, 1000],
            index=3,
            key="convert_preview_limit_select",
        )
        st.session_state["convert_preview_limit"] = preview_limit

    with convert_toolbar_col2:
        export_zip = st.checkbox(
            "Download as ZIP",
            value=False,
            key="convert_export_zip",
        )

    with convert_toolbar_col3:
        st.write("")

    if not uploaded:
        st.info("Upload a file to begin.")
        return

    show_success(f"Loaded: {uploaded.name}")

    file_name_lower = uploaded.name.lower()

    if file_name_lower.endswith(".csv") or file_name_lower.endswith(".xlsx"):
        try:
            df = try_load_spreadsheet(uploaded)
        except Exception as exc:
            show_error(str(exc))
            return

        if df.empty:
            show_error("The spreadsheet loaded, but it appears to be empty.")
            return

        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown("### Spreadsheet preview")
        st.dataframe(df.head(st.session_state["convert_preview_limit"]), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        cols1, cols2, cols3 = st.columns(3)

        all_columns = list(df.columns)

        with cols1:
            name_col = st.selectbox(
                "Name column",
                options=all_columns,
                key="convert_name_column",
            )

        with cols2:
            phone_col = st.selectbox(
                "Phone column",
                options=all_columns,
                key="convert_phone_column",
            )

        with cols3:
            email_col = st.selectbox(
                "Email column",
                options=all_columns,
                key="convert_email_column",
            )

        action_col1, action_col2 = st.columns([1, 4])

        with action_col1:
            run_convert = st.button("Convert now", use_container_width=True, key="convert_run_button")

        if run_convert:
            try:
                contacts = df_to_contacts(df, name_col, phone_col, email_col)
                contacts = safe_list(contacts)
                st.session_state["convert_contacts"] = contacts
                show_success(f"Converted {len(contacts):,} contacts.")
            except Exception as exc:
                show_error(f"Conversion failed: {exc}")

    else:
        left, right = st.columns([1, 4])
        with left:
            run_general_import = st.button(
                "Read file",
                use_container_width=True,
                key="convert_read_general_button",
            )

        if run_general_import:
            try:
                contacts = try_parse_general_file(uploaded)
                st.session_state["convert_contacts"] = contacts
                show_success(f"Imported {len(contacts):,} contacts.")
            except Exception as exc:
                show_error(str(exc))

    contacts = st.session_state["convert_contacts"]

    if not contacts:
        return

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### Converted contact preview")
    preview_df = contacts_to_df(contacts, st.session_state["convert_preview_limit"])
    st.dataframe(preview_df, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    vcf_bytes = build_vcf_download_data(contacts)

    download_col1, download_col2, download_col3 = st.columns([1.2, 1.2, 4])

    with download_col1:
        st.download_button(
            "Download VCF",
            data=vcf_bytes,
            file_name="contacts.vcf",
            mime="text/vcard",
            key="convert_download_vcf_button",
            use_container_width=True,
        )

    with download_col2:
        if export_zip:
            zip_bytes = build_zip_of_vcf("contacts.vcf", vcf_bytes)
            st.download_button(
                "Download ZIP",
                data=zip_bytes,
                file_name="contacts.zip",
                mime="application/zip",
                key="convert_download_zip_button",
                use_container_width=True,
            )


# ============================================================
# Merge tab
# ============================================================
def merge_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Merge VCFs")
    st.caption("Upload multiple VCF files, optionally de-duplicate during merge, then download one combined VCF.")
    st.markdown("</div>", unsafe_allow_html=True)

    files = st.file_uploader(
        "Drag and drop one or more VCF files here",
        type=["vcf"],
        accept_multiple_files=True,
        key="merge_upload_files",
    )

    opts1, opts2, opts3 = st.columns([1.2, 1.2, 1.2])

    with opts1:
        merge_dedupe = st.checkbox(
            "Remove exact duplicates",
            value=False,
            key="merge_remove_duplicates_checkbox",
        )

    with opts2:
        merge_smart_merge = st.checkbox(
            "Smart merge duplicates",
            value=False,
            key="merge_smart_merge_checkbox",
        )

    with opts3:
        merge_preview_limit = st.selectbox(
            "Preview rows",
            options=[25, 50, 100, 250, 500, 1000],
            index=3,
            key="merge_preview_limit_select",
        )
        st.session_state["merge_preview_limit"] = merge_preview_limit

    if not files:
        st.info("Upload one or more VCF files to begin.")
        return

    st.write(f"Loaded {len(files)} file(s).")

    run_merge_col1, run_merge_col2 = st.columns([1, 5])
    with run_merge_col1:
        run_merge = st.button("Merge files", use_container_width=True, key="merge_run_button")

    if run_merge:
        all_contacts: List[Any] = []

        for idx, file in enumerate(files):
            try:
                parsed = try_parse_vcf_file(file)
                all_contacts.extend(parsed)
            except Exception as exc:
                show_error(f"Failed on file {idx + 1} ({file.name}): {exc}")
                return

        merged_contacts = all_contacts

        if merge_dedupe or merge_smart_merge:
            try:
                groups = find_duplicates(merged_contacts)
            except Exception as exc:
                show_error(f"Duplicate scan failed: {exc}")
                return

            if merge_smart_merge:
                final_contacts: List[Any] = []
                used_ids = set()

                for group in groups:
                    if len(group) <= 1:
                        contact = group[0]
                        final_contacts.append(contact)
                        used_ids.add(id(contact))
                    else:
                        final_contacts.append(merge_contacts(group))
                        for item in group:
                            used_ids.add(id(item))

                for contact in merged_contacts:
                    if id(contact) not in used_ids:
                        final_contacts.append(contact)

                merged_contacts = final_contacts
            else:
                final_contacts = []
                used_ids = set()

                for group in groups:
                    if not group:
                        continue
                    keep = group[0]
                    final_contacts.append(keep)
                    for item in group:
                        used_ids.add(id(item))

                for contact in merged_contacts:
                    if id(contact) not in used_ids:
                        final_contacts.append(contact)

                merged_contacts = final_contacts

        st.session_state["merge_contacts"] = merged_contacts
        show_success(f"Prepared {len(merged_contacts):,} contacts for export.")

    contacts = st.session_state["merge_contacts"]
    if not contacts:
        return

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### Merged contact preview")
    st.dataframe(
        contacts_to_df(contacts, st.session_state["merge_preview_limit"]),
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    vcf_bytes = build_vcf_download_data(contacts)

    download_col1, download_col2 = st.columns([1.1, 4])
    with download_col1:
        st.download_button(
            "Download merged VCF",
            data=vcf_bytes,
            file_name="merged_contacts.vcf",
            mime="text/vcard",
            key="merge_download_button",
            use_container_width=True,
        )


# ============================================================
# Dedupe tab
# ============================================================
def render_duplicate_group(group_index: int, group: List[Any]) -> None:
    st.markdown('<div class="group-card">', unsafe_allow_html=True)
    st.markdown(f"### Duplicate group {group_index + 1}")

    if f"dedupe_keep_choice_{group_index}" not in st.session_state:
        st.session_state[f"dedupe_keep_choice_{group_index}"] = 0

    labels = [f"{idx + 1}. {get_contact_display_name(contact)}" for idx, contact in enumerate(group)]
    keep_choice = st.radio(
        "Choose which contact to keep for this group",
        options=list(range(len(group))),
        format_func=lambda x: labels[x],
        horizontal=False,
        key=f"dedupe_keep_choice_{group_index}",
    )

    cols = st.columns(len(group))
    for j, contact in enumerate(group):
        with cols[j]:
            render_contact_card(contact, prefix=f"group_{group_index}_contact_{j}")
            if j == keep_choice:
                st.success("Selected to keep")

    st.markdown("</div>", unsafe_allow_html=True)


def dedupe_tab() -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("VCF De-duplicate")
    st.caption(
        "Upload a VCF, find duplicate groups, review them in a card layout, then export a cleaned file."
    )
    st.markdown("</div>", unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Drag and drop a VCF file here",
        type=["vcf"],
        accept_multiple_files=False,
        key="dedupe_upload_file",
    )

    rules1, rules2, rules3, rules4 = st.columns([1, 1, 1, 1.25])

    with rules1:
        match_by_email = st.checkbox(
            "Match by email",
            value=True,
            key="dedupe_match_by_email_checkbox",
        )

    with rules2:
        match_by_phone = st.checkbox(
            "Match by phone",
            value=True,
            key="dedupe_match_by_phone_checkbox",
        )

    with rules3:
        match_by_name = st.checkbox(
            "Match by name",
            value=True,
            key="dedupe_match_by_name_checkbox",
        )

    with rules4:
        smart_merge = st.checkbox(
            "Smart merge duplicates",
            value=False,
            key="dedupe_smart_merge_checkbox",
        )

    dedupe_preview_limit = st.selectbox(
        "Preview rows",
        options=[25, 50, 100, 250, 500, 1000],
        index=3,
        key="dedupe_preview_limit_select",
    )
    st.session_state["dedupe_preview_limit"] = dedupe_preview_limit

    if not uploaded:
        st.info("Upload a VCF file to begin.")
        return

    load_col1, load_col2 = st.columns([1, 5])
    with load_col1:
        load_vcf = st.button("Load VCF", use_container_width=True, key="dedupe_load_button")

    if load_vcf:
        try:
            contacts = try_parse_vcf_file(uploaded)
            st.session_state["dedupe_input_contacts"] = contacts
            st.session_state["dedupe_groups"] = []
            st.session_state["dedupe_result_contacts"] = []
            show_success(f"Loaded {len(contacts):,} contacts.")
        except Exception as exc:
            show_error(str(exc))
            return

    input_contacts = st.session_state["dedupe_input_contacts"]
    if not input_contacts:
        return

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### Loaded VCF preview")
    st.dataframe(
        contacts_to_df(input_contacts, st.session_state["dedupe_preview_limit"]),
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    scan_col1, scan_col2 = st.columns([1.2, 4])
    with scan_col1:
        run_scan = st.button("Find duplicates", use_container_width=True, key="dedupe_scan_button")

    if run_scan:
        try:
            groups = find_duplicates(input_contacts)

            filtered_groups = []
            for group in groups:
                if len(group) > 1:
                    filtered_groups.append(group)

            st.session_state["dedupe_groups"] = filtered_groups
            show_success(f"Found {len(filtered_groups):,} duplicate group(s).")
        except Exception as exc:
            show_error(f"Duplicate scan failed: {exc}")
            return

    dup_groups = st.session_state["dedupe_groups"]

    if dup_groups:
        for group_index, group in enumerate(dup_groups):
            render_duplicate_group(group_index, group)

        apply_col1, apply_col2 = st.columns([1.25, 4])
        with apply_col1:
            apply_dedupe = st.button(
                "Apply choices",
                use_container_width=True,
                key="dedupe_apply_choices_button",
            )

        if apply_dedupe:
            result_contacts: List[Any] = []
            used_ids = set()

            for group_index, group in enumerate(dup_groups):
                if smart_merge:
                    merged = merge_contacts(group)
                    result_contacts.append(merged)
                    for item in group:
                        used_ids.add(id(item))
                else:
                    keep_idx = st.session_state.get(f"dedupe_keep_choice_{group_index}", 0)
                    keep_idx = max(0, min(keep_idx, len(group) - 1))
                    chosen = group[keep_idx]
                    result_contacts.append(chosen)
                    for item in group:
                        used_ids.add(id(item))

            for contact in input_contacts:
                if id(contact) not in used_ids:
                    result_contacts.append(contact)

            st.session_state["dedupe_result_contacts"] = result_contacts
            show_success(f"Prepared {len(result_contacts):,} cleaned contacts.")

    result_contacts = st.session_state["dedupe_result_contacts"]

    if not result_contacts:
        return

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown("### Cleaned contact preview")
    st.dataframe(
        contacts_to_df(result_contacts, st.session_state["dedupe_preview_limit"]),
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    vcf_bytes = build_vcf_download_data(result_contacts)

    export_col1, export_col2 = st.columns([1.2, 4])
    with export_col1:
        st.download_button(
            "Download cleaned VCF",
            data=vcf_bytes,
            file_name="deduplicated_contacts.vcf",
            mime="text/vcard",
            key="dedupe_download_button",
            use_container_width=True,
        )

    rule_summary = []
    if match_by_email:
        rule_summary.append("email")
    if match_by_phone:
        rule_summary.append("phone")
    if match_by_name:
        rule_summary.append("name")

    if rule_summary:
        st.caption("Selected matching rules: " + ", ".join(rule_summary))
    else:
        st.caption("No match rules selected in UI. The underlying duplicate function still follows your core logic.")


# ============================================================
# Help tab
# ============================================================
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

        **Notes**
        - This Streamlit version is browser-based
        - Downloads are handled through the browser
        - No desktop save dialogs are used on Streamlit Cloud
        """
    )
    st.markdown("</div>", unsafe_allow_html=True)


# ============================================================
# Main
# ============================================================
def main() -> None:
    top_header()

    stat_row(
        convert_count=len(st.session_state["convert_contacts"]),
        merge_count=len(st.session_state["merge_contacts"]),
        dedupe_count=len(st.session_state["dedupe_result_contacts"]),
        dup_groups=len(st.session_state["dedupe_groups"]),
    )

    tab_convert, tab_merge, tab_dedupe, tab_help = st.tabs(
        ["Convert to VCF", "Merge VCFs", "VCF De-duplicate", "Help"]
    )

    with tab_convert:
        convert_tab()

    with tab_merge:
        merge_tab()

    with tab_dedupe:
        dedupe_tab()

    with tab_help:
        help_tab()

    st.markdown(
        '<div class="footer-note">ROSE COMMUNICATIONS GROUP LTD • Streamlit rebuild</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
