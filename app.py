from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import streamlit as st

from rcuk_contact_manager.core import (
    APP_NAME,
    APP_VERSION,
    COPYRIGHT_TEXT,
    ImportedData,
    NormalizationOptions,
    TARGET_FIELDS,
    apply_normalization_all,
    best_display_name,
    build_cards_from_mapped_rows,
    build_vcard,
    export_convert_bundle,
    export_dedupe_bundle,
    export_merge_bundle,
    group_duplicates,
    guess_target,
    import_from_csv_text,
    import_from_ib_bytes,
    import_from_nbf_or_zip_bytes,
    import_from_vcf_bytes,
    import_from_xlsx_bytes,
    parse_vcard_fields,
    smart_merge_cards,
    unique_preserve,
)

st.set_page_config(page_title=APP_NAME, page_icon="📇", layout="wide")


# ---------------------------- Styling ----------------------------

def inject_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1400px;}
        .hero {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 55%, #334155 100%);
            border-radius: 24px;
            padding: 28px 30px;
            color: white;
            box-shadow: 0 14px 40px rgba(15, 23, 42, 0.25);
            margin-bottom: 1rem;
        }
        .hero h1 {margin: 0; font-size: 2rem; line-height: 1.1;}
        .hero p {margin: .55rem 0 0 0; opacity: .92; font-size: 1rem;}
        .mini-card {
            background: #ffffff;
            border: 1px solid rgba(15,23,42,.08);
            border-radius: 20px;
            padding: 14px 16px;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
        }
        .section-title {font-size: 1.1rem; font-weight: 700; margin-top: .2rem; margin-bottom: .4rem;}
        .muted {color: #64748b;}
        .contact-card {
            border: 1px solid rgba(15,23,42,.09);
            border-radius: 18px;
            padding: 14px 16px;
            background: #fff;
            margin-bottom: 10px;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
        }
        .footer {text-align:center; color:#64748b; font-size:.86rem; padding-top:1rem;}
        .small-label {font-size:.82rem;color:#64748b;text-transform:uppercase;letter-spacing:.04em;}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------- Helpers ----------------------------

def init_state() -> None:
    defaults = {
        "convert_imported": ImportedData(),
        "convert_mapping": {},
        "convert_selected_index": 0,
        "merge_cards": [],
        "merge_sources": [],
        "dedupe_cards": [],
        "dedupe_filename": "",
        "dedupe_keep": {},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value



def norm_options_ui(prefix: str) -> NormalizationOptions:
    with st.container(border=True):
        st.markdown('<div class="section-title">Normalisation</div>', unsafe_allow_html=True)
        phone_mode = st.radio(
            "Phone formatting",
            ["keep", "uk", "international"],
            index=0,
            horizontal=True,
            key=f"{prefix}_phone_mode",
            format_func=lambda x: {
                "keep": "Keep as-is",
                "uk": "UK format (0...)",
                "international": "International (+44)",
            }[x],
        )
        c1, c2, c3 = st.columns(3)
        trim_spaces = c1.checkbox("Trim spaces", key=f"{prefix}_trim_spaces")
        unify_case = c2.checkbox("Unify casing", key=f"{prefix}_unify_case")
        strip_name_numbers = c3.checkbox("Remove numbers after names", key=f"{prefix}_strip_name_numbers")
    return NormalizationOptions(
        phone_mode=phone_mode,
        trim_spaces=trim_spaces,
        unify_case=unify_case,
        strip_name_numbers=strip_name_numbers,
    )



def cards_to_dataframe(cards: list[str], source_name: str = "") -> pd.DataFrame:
    rows = []
    for i, c in enumerate(cards):
        f = parse_vcard_fields(c)
        rows.append(
            {
                "#": i + 1,
                "Name": str(f.get("fn") or ""),
                "Phone": ", ".join(f.get("tels") or []),
                "Email": ", ".join(f.get("emails") or []),
                "Source": source_name,
            }
        )
    return pd.DataFrame(rows)



def make_zip(files: dict[str, bytes]) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return bio.getvalue()



def import_uploaded_file(uploaded) -> ImportedData:
    filename = uploaded.name
    ext = Path(filename).suffix.lower()
    data = uploaded.getvalue()
    imp = ImportedData(cards=[], sources=[], kind=ext, filename=filename)

    if ext == ".vcf":
        imp.cards = import_from_vcf_bytes(data)
        imp.sources = [(filename, len(imp.cards))]
    elif ext == ".csv":
        rows, headers = import_from_csv_text(data.decode("utf-8", errors="ignore"))
        imp.raw_rows, imp.headers = rows, headers
    elif ext in (".xlsx", ".xlsm"):
        rows, headers = import_from_xlsx_bytes(data)
        imp.raw_rows, imp.headers = rows, headers
    elif ext in (".nbf", ".nbu", ".zip"):
        cards, per = import_from_nbf_or_zip_bytes(data, filename)
        imp.cards = cards
        imp.sources = per
    elif ext == ".ib":
        cards, per = import_from_ib_bytes(data, filename)
        imp.cards = cards
        imp.sources = per
    else:
        imp.cards = import_from_vcf_bytes(data)
        imp.sources = [(filename, len(imp.cards))]
    return imp



def render_metrics(total: int, shown: int, source_count: int) -> None:
    c1, c2, c3 = st.columns(3)
    c1.metric("Loaded contacts", f"{total:,}")
    c2.metric("Shown", f"{shown:,}")
    c3.metric("Sources", f"{source_count:,}")



def footer() -> None:
    st.markdown(f'<div class="footer">{COPYRIGHT_TEXT} · {APP_VERSION}</div>', unsafe_allow_html=True)


# ---------------------------- Sections ----------------------------

def render_header() -> None:
    st.markdown(
        f"""
        <div class="hero">
            <div class="small-label">RCUK Contact Tools</div>
            <h1>{APP_NAME}</h1>
            <p>Streamlit rebuild with the original contact-reading and conversion logic preserved, now wrapped in a cleaner desktop dashboard.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )



def convert_tab() -> None:
    st.subheader("Convert to VCF")
    left, right = st.columns([1, 1.55], gap="large")

    with left:
        uploaded = st.file_uploader(
            "Drop or browse a contacts file",
            type=["vcf", "csv", "xlsx", "xlsm", "nbf", "nbu", "zip", "ib"],
            key="convert_upload",
            help="Supports VCF, CSV, XLSX, Nokia NBF/NBU/ZIP and IB files.",
        )
        if uploaded is not None:
            try:
                st.session_state.convert_imported = import_uploaded_file(uploaded)
                if st.session_state.convert_imported.headers:
                    st.session_state.convert_mapping = {
                        h: guess_target(h) for h in st.session_state.convert_imported.headers
                    }
            except Exception as e:
                st.error(f"Import failed: {e}")

        options = norm_options_ui("convert")

        with st.container(border=True):
            st.markdown('<div class="section-title">Export</div>', unsafe_allow_html=True)
            export_mode = st.radio(
                "Export mode",
                ["merged", "per_contact", "split_batches"],
                horizontal=True,
                format_func=lambda x: {
                    "merged": "One merged VCF",
                    "per_contact": "One file per contact",
                    "split_batches": "Split into batches",
                }[x],
                key="convert_export_mode",
            )
            batch_size = st.number_input(
                "Batch size",
                min_value=1,
                value=500,
                disabled=export_mode != "split_batches",
                key="convert_batch_size",
            )

    imp = st.session_state.convert_imported

    if imp.raw_rows is not None and imp.headers:
        with right:
            st.markdown("#### Column mapping")
            mapping_cols = st.columns(2)
            for i, header in enumerate(imp.headers):
                col = mapping_cols[i % 2]
                with col:
                    st.session_state.convert_mapping[header] = st.selectbox(
                        header,
                        TARGET_FIELDS,
                        index=TARGET_FIELDS.index(st.session_state.convert_mapping.get(header, guess_target(header))),
                        key=f"map_{header}",
                    )
            if st.button("Build contacts from spreadsheet", type="primary"):
                cards = build_cards_from_mapped_rows(imp.raw_rows, st.session_state.convert_mapping)
                st.session_state.convert_imported.cards = cards
                st.session_state.convert_imported.sources = [(imp.filename or "spreadsheet", len(cards))]
                st.success(f"Built {len(cards):,} contacts from spreadsheet columns.")

            preview_df = pd.DataFrame(imp.raw_rows[:10]) if imp.raw_rows else pd.DataFrame()
            if not preview_df.empty:
                st.markdown("#### Spreadsheet preview")
                st.dataframe(preview_df, use_container_width=True, hide_index=True)

    if not imp.cards:
        with right:
            st.info("Load a file to preview contacts here.")
        return

    normalized = apply_normalization_all(imp.cards, options)
    search = right.text_input("Search contacts", placeholder="Name, phone, email…")
    rows = []
    for i, c in enumerate(normalized):
        f = parse_vcard_fields(c)
        row = {
            "idx": i,
            "Name": str(f.get("fn") or ""),
            "Phone": ", ".join(f.get("tels") or []),
            "Email": ", ".join(f.get("emails") or []),
            "Source": imp.sources[0][0] if imp.sources else imp.kind,
        }
        blob = " ".join(str(v) for v in row.values()).lower()
        if search.strip() and search.strip().lower() not in blob:
            continue
        rows.append(row)

    render_metrics(len(imp.cards), len(rows), len(imp.sources or []))
    right.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    selected_idx = left.number_input(
        "Select contact to view / edit",
        min_value=1,
        max_value=max(1, len(imp.cards)),
        value=min(max(1, int(st.session_state.convert_selected_index) + 1), max(1, len(imp.cards))),
        step=1,
    ) - 1
    st.session_state.convert_selected_index = selected_idx

    raw_card = imp.cards[selected_idx]
    raw_fields = parse_vcard_fields(raw_card)
    with left.container(border=True):
        st.markdown('<div class="section-title">Edit contact</div>', unsafe_allow_html=True)
        name = st.text_input("Name", value=str(raw_fields.get("fn") or ""), key="edit_name")
        phones = st.text_input("Phones (comma separated)", value=", ".join(raw_fields.get("tels") or []), key="edit_phones")
        emails = st.text_input("Emails (comma separated)", value=", ".join(raw_fields.get("emails") or []), key="edit_emails")
        if st.button("Save contact changes", use_container_width=True):
            imp.cards[selected_idx] = build_vcard(
                name.strip(),
                unique_preserve([p.strip() for p in phones.split(",")]),
                unique_preserve([e.strip() for e in emails.split(",")]),
            )
            st.success("Contact updated.")

    with right.expander("VCF preview for selected contact", expanded=False):
        st.code(normalized[selected_idx], language="text")

    bundle = export_convert_bundle(imp, options, export_mode, int(batch_size))
    zip_bytes = make_zip(bundle.files)
    left.download_button(
        "Download export ZIP",
        data=zip_bytes,
        file_name="rcuk_contact_export.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
    )



def merge_tab() -> None:
    st.subheader("Merge VCFs")
    left, right = st.columns([1, 1.45], gap="large")
    with left:
        uploads = st.file_uploader(
            "Drop one or more VCF files",
            type=["vcf"],
            accept_multiple_files=True,
            key="merge_uploads",
        )
        if uploads:
            merge_cards: list[str] = []
            merge_sources: list[tuple[str, int]] = []
            for up in uploads:
                cards = import_from_vcf_bytes(up.getvalue())
                merge_cards.extend(cards)
                merge_sources.append((up.name, len(cards)))
            st.session_state.merge_cards = merge_cards
            st.session_state.merge_sources = merge_sources

        options = norm_options_ui("merge")
        remove_duplicates = st.checkbox("Remove duplicates", value=True)
        smart_merge = st.checkbox("Smart merge duplicates (combine phones/emails)", value=False)

    cards = st.session_state.merge_cards
    sources = st.session_state.merge_sources
    if not cards:
        right.info("Upload VCF files to merge them here.")
        return

    render_metrics(len(cards), len(cards), len(sources))
    src_df = pd.DataFrame(sources, columns=["File", "Contacts"])
    left.dataframe(src_df, use_container_width=True, hide_index=True)

    preview_bundle = export_merge_bundle(cards, options, remove_duplicates, smart_merge)
    merged_text = preview_bundle.files["merged_contacts.vcf"].decode("utf-8", errors="ignore")
    merged_cards = import_from_vcf_bytes(merged_text.encode("utf-8"))
    right.dataframe(cards_to_dataframe(merged_cards, "Merged output"), use_container_width=True, hide_index=True)
    right.download_button(
        "Download merged ZIP",
        data=make_zip(preview_bundle.files),
        file_name="rcuk_merged_contacts.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
    )



def dedupe_tab() -> None:
    st.subheader("VCF De-duplicate")
    top_left, top_right = st.columns([1, 1.3], gap="large")
    with top_left:
        uploaded = st.file_uploader("Drop a VCF to review duplicates", type=["vcf"], key="dedupe_upload")
        if uploaded is not None:
            st.session_state.dedupe_cards = import_from_vcf_bytes(uploaded.getvalue())
            st.session_state.dedupe_filename = uploaded.name
            st.session_state.dedupe_keep = {}

        options = norm_options_ui("dedupe")
        match_email = st.checkbox("Match by email", value=True)
        match_phone = st.checkbox("Match by phone", value=True)
        match_name = st.checkbox("Match by name", value=True)
        smart_merge = st.checkbox("Smart merge duplicates (combine phones/emails)", value=False)

    cards = st.session_state.dedupe_cards
    if not cards:
        top_right.info("Upload a VCF to detect duplicate groups.")
        return

    dup_groups = group_duplicates(cards, match_email, match_phone, match_name)
    for gi, grp in enumerate(dup_groups):
        st.session_state.dedupe_keep.setdefault(gi, grp[0])

    c1, c2, c3 = st.columns(3)
    c1.metric("Contacts in file", f"{len(cards):,}")
    c2.metric("Duplicate groups", f"{len(dup_groups):,}")
    c3.metric("Potential duplicates", f"{sum(len(g) for g in dup_groups):,}")

    if not dup_groups:
        top_right.success("No duplicate groups found with the current rules.")
    else:
        st.markdown("#### Review duplicate groups")
        for gi, grp in enumerate(dup_groups):
            with st.container(border=True):
                st.markdown(f"**Group {gi + 1}** · {len(grp)} copies")
                options_list = []
                captions = []
                for idx in grp:
                    f = parse_vcard_fields(cards[idx])
                    label = str(f.get("fn") or f"Contact {idx + 1}")
                    options_list.append(idx)
                    captions.append(label)
                selected = st.radio(
                    "Keep this copy",
                    options_list,
                    index=options_list.index(st.session_state.dedupe_keep.get(gi, grp[0])),
                    key=f"keep_group_{gi}",
                    horizontal=True,
                    format_func=lambda x: best_display_name(cards[x]) or f"Contact {x + 1}",
                    label_visibility="collapsed",
                )
                st.session_state.dedupe_keep[gi] = selected
                cols = st.columns(len(grp))
                for col, idx in zip(cols, grp):
                    f = parse_vcard_fields(cards[idx])
                    with col:
                        keep_badge = "✅ Keeping" if idx == selected and not smart_merge else "Candidate"
                        st.markdown(f"**{keep_badge}**")
                        st.markdown(f"**{str(f.get('fn') or 'No name')}**")
                        st.caption("Phones")
                        st.write(", ".join(f.get("tels") or []) or "—")
                        st.caption("Emails")
                        st.write(", ".join(f.get("emails") or []) or "—")
                        with st.expander("Raw VCF"):
                            st.code(cards[idx], language="text")
                if smart_merge:
                    merged_preview = smart_merge_cards([cards[i] for i in grp])
                    with st.expander("Smart merge preview"):
                        st.code(merged_preview, language="text")

    bundle = export_dedupe_bundle(cards, options, dup_groups, st.session_state.dedupe_keep, smart_merge)
    top_left.download_button(
        "Download de-duplicated ZIP",
        data=make_zip(bundle.files),
        file_name="rcuk_deduplicated_contacts.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
    )

    out_cards = import_from_vcf_bytes(bundle.files["deduplicated.vcf"])
    top_right.dataframe(cards_to_dataframe(out_cards, "Deduplicated output"), use_container_width=True, hide_index=True)



def help_tab() -> None:
    st.subheader("Help")
    c1, c2 = st.columns(2, gap="large")
    with c1.container(border=True):
        st.markdown("### What this rebuild keeps")
        st.write(
            "The contact parsing, conversion, normalization, Nokia backup handling, CSV/XLSX mapping, merge logic and duplicate-detection logic were preserved from the original app and wrapped in a Streamlit UI."
        )
        st.write("Use **Convert to VCF** for single-file imports, **Merge VCFs** for combining exports, and **VCF De-duplicate** for cleanup.")
    with c2.container(border=True):
        st.markdown("### Best use on desktop")
        st.write(
            "This version is designed for desktop use, with drag-and-drop upload, wide contact tables, side-by-side duplicate review, and ZIP downloads for all exports."
        )
        st.write("Spreadsheet files still require column mapping before contacts are built.")


# ---------------------------- App ----------------------------

def main() -> None:
    inject_css()
    init_state()
    render_header()
    tab1, tab2, tab3, tab4 = st.tabs(["Convert to VCF", "Merge VCFs", "VCF De-duplicate", "Help"])
    with tab1:
        convert_tab()
    with tab2:
        merge_tab()
    with tab3:
        dedupe_tab()
    with tab4:
        help_tab()
    footer()


if __name__ == "__main__":
    main()
