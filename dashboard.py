
import streamlit as st
import asyncio
import pandas as pd
import io
import datetime
from pandas import ExcelWriter
from crawler_async import crawl
from sitemap_loader import load_sitemap
from crawler_spider import crawl_domain

LOGO_URL_LARGE = "https://www.berendsohn.de/logos/bag-logo.svg"
LOGO_URL_SMALL = "https://www.berendsohn.de/logos/bag-logo.svg"

st.logo(
    LOGO_URL_LARGE,
    icon_image=LOGO_URL_SMALL,
)

today_str = datetime.date.today().strftime("%Y%m%d")

st.set_page_config(page_title="SEO-Checker", layout="wide")
st.title("SEO-Checker")
st.text("Prüfe jetzt eine URL, um wertvolle Einblicke in die wichtigsten SEO-Metriken zu erhalten.")

if "url_list" not in st.session_state:
    st.session_state["url_list"] = []
if "result_df" not in st.session_state:
    st.session_state["result_df"] = None

tab_crawler, tab_manual, tab_sitemap = st.tabs(["Crawler", "Manuelle Eingabe", "Sitemap laden"])

with tab_crawler:
    crawl_url = st.text_input("Start-URL zum Crawlen", placeholder="https://example.com")
    max_pages = st.slider("Max. Anzahl zu crawlender Seiten", 10, 500, 100)
    if st.button("Domain crawlen"):
        if crawl_url:
            with st.spinner("Crawle interne Seiten …"):
                st.session_state["url_list"] = asyncio.run(crawl_domain(crawl_url, max_pages))
            st.success(f"{len(st.session_state['url_list'])} interne Seiten gefunden.")

with tab_manual:
    textarea = st.text_area("Eine URL pro Zeile")
    if st.button("URLs übernehmen"):
        urls = [u.strip() for u in textarea.splitlines() if u.strip()]
        st.session_state["url_list"] = urls
        st.success(f"{len(urls)} URLs übernommen.")

with tab_sitemap:
    sitemap_url = st.text_input("Sitemap-URL", placeholder="https://example.com/sitemap.xml")
    if st.button("Sitemap laden"):
        if sitemap_url:
            with st.spinner("Sitemap wird geladen …"):
                st.session_state["url_list"] = load_sitemap(sitemap_url)
            st.success(f"{len(st.session_state['url_list'])} URLs gefunden.")

st.divider()

if st.session_state["url_list"]:
    if st.button("Analyse starten"):
        progress = st.progress(0, "Starte Analyse …")
        async def run_crawl(urls):
            df_all = []
            for idx, url in enumerate(urls, 1):
                df_all.append(await crawl([url]))
                progress.progress(idx / len(urls), text=f"Analysiere {idx}/{len(urls)}")
            return pd.concat(df_all, ignore_index=True)

        st.session_state["result_df"] = asyncio.run(run_crawl(st.session_state["url_list"]))
        progress.empty()

def row_style(row):
    if str(row.get("HTTP Status", "")).startswith(("4", "5")):
        return ["background-color: #f8d7da"] * len(row)
    if "noindex" in str(row.get("Status", "")).lower() or row.get("Robots Policy") == "Disallowed":
        return ["background-color: #fff4cd"] * len(row)
    return [""] * len(row)

if st.session_state["result_df"] is not None:
    df = st.session_state["result_df"].drop(
        columns=[c for c in ("Hinweis",) if c in st.session_state["result_df"].columns]
    )

    try:
        styled = df.style.apply(row_style, axis=1)
        st.dataframe(styled, use_container_width=True)
    except AttributeError:
        st.dataframe(df, use_container_width=True)

    # CSV Export
    csv = df.to_csv(index=False).encode()
    st.download_button(
        label="CSV herunterladen",
        data=csv,
        file_name=f"seo_checker_results_{today_str}.csv",
        mime="text/csv"
    )

    # Excel Export
    excel_buffer = io.BytesIO()
    with ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="SEO Ergebnisse")
    st.download_button(
        label="Excel herunterladen",
        data=excel_buffer.getvalue(),
        file_name=f"seo_checker_results_{today_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.markdown("---")
    st.subheader("Erklärungen der SEO-Metriken")

    with st.expander("Indexierbarkeit (noindex)"):
        st.write("""
**Was es ist**  
"Indexierbarkeit" bedeutet, dass Suchmaschinen eine Webseite "lesen" und in ihren Index aufnehmen können, damit sie in den Suchergebnissen erscheinen kann.  
**"Noindex"** ist eine Anweisung an Suchmaschinen, eine bestimmte Seite nicht zu indexieren.

**Warum es wichtig ist**  
Wenn eine Seite auf "noindex" gesetzt ist, erscheint sie **nicht in den Google-Suchergebnissen**.  
Das ist wichtig für Seiten, die nicht öffentlich zugänglich sein sollen (z. B. Login-Bereiche, interne Dankeseiten nach einer Anmeldung).

**Schlechtes Beispiel**  
Die Startseite, eine wichtige Produktseite oder Landingpage ist auf "noindex" gesetzt.  
→ Erklärung: Diese Seite wird niemals von potenziellen Kunden über die Suche gefunden. Das kostet Sichtbarkeit und Umsatz.

**Gutes Beispiel**  
Die "Danke für Ihre Anfrage"-Seite nach dem Absenden eines Kontaktformulars ist auf "noindex" gesetzt.  
→ Erklärung: Diese Seite hat nur nach einer Aktion Bedeutung und bietet sonst keinen Mehrwert für Suchende.
""")

    with st.expander("Robots-Policy (robots.txt)"):
        st.write("""
**Was es ist**  
Die robots.txt ist eine kleine Datei auf Ihrem Webserver, die Suchmaschinen mitteilt, welche Bereiche Ihrer Webseite sie crawlen (besuchen und lesen) dürfen und welche nicht.  
Mit `/disallow` verbietet man bestimmten Suchmaschinen-Bots den Zugriff auf bestimmte Verzeichnisse oder Seiten.

**Warum es wichtig ist**  
Sie steuern damit, welche Teile Ihrer Website von Suchmaschinen überhaupt erst angesehen werden können.  
Dies ist nützlich, um unnötige oder doppelte Inhalte vom Crawling auszuschließen und das Crawling-Budget der Suchmaschinen effizient zu nutzen.

**Schlechtes Beispiel**  
Die gesamte Webseite wird per robots.txt disallowiert oder wichtige Bereiche wie der Blog oder Produktkategorien.  
→ Erklärung: Wenn Sie Suchmaschinen den Zugriff auf wichtige Teile Ihrer Seite verbieten, können diese Inhalte nicht indexiert werden und erscheinen somit nicht in den Suchergebnissen. Das ist ein großer Selbstschuss für die Sichtbarkeit.

**Gutes Beispiel**  
Das Verzeichnis mit internen Dokumenten oder Testseiten (`Disallow: /intern/` oder `Disallow: /testseite/`) wird in der robots.txt ausgeschlossen.  
→ Erklärung: Hier wird gezielt verhindert, dass nicht‑öffentliche oder redundante Inhalte von Suchmaschinen indexiert werden, was sinnvoll ist und die Effizienz des Crawlings verbessert.
""")

    with st.expander("Title (Titel-Tag)"):
        st.write("""
**Was es ist**  
Der Title ist der wichtigste Textbaustein einer Webseite für Suchmaschinen und Nutzer.  
Er erscheint im Browser-Tab und als Überschrift in den Suchergebnissen.

**Warum er wichtig ist**  
Suchmaschinen nutzen den Title, um zu verstehen, worum es auf Ihrer Seite geht.  
Nutzer klicken darauf, um zu entscheiden, ob die Seite relevant ist.

**Schlechtes Beispiel**  
„Startseite“ oder „Willkommen auf unserer Homepage“  
→ Erklärung: Diese Titel sagen Suchmaschinen und Nutzern nichts darüber, was Ihre Firma macht oder anbietet. Sie sind generisch und nutzlos.

**Gutes Beispiel**  
„SEO Beratung Hamburg – Experten für Suchmaschinenoptimierung | Ihre Agentur XYZ“  
→ Erklärung: Hier ist das wichtigste Keyword („SEO Beratung“) am Anfang, der Standort („Hamburg“) ist enthalten und der Nutzer versteht sofort, worum es geht.  
Ziel ist es, das Fokus-Keyword möglichst weit vorn zu platzieren und einen Mehrwert für den Nutzer zu kommunizieren.
""")

    with st.expander("Meta-Description"):
        st.write("""
**Was es ist**  
Das ist der kurze Textausschnitt, der unter dem Title in den Suchergebnissen angezeigt wird.  
Er ist wie ein kleiner Werbetext für Ihre Seite.

**Warum er wichtig ist**  
Obwohl die Meta-Description kein direktes Ranking-Kriterium ist, beeinflusst sie stark, ob Nutzer auf Ihr Suchergebnis klicken.  
Eine gute Beschreibung weckt Interesse.

**Schlechtes Beispiel**  
„Eine kurze Beschreibung unserer Webseite, die viele Inhalte bietet.“  
→ Erklärung: Dieser Text ist vage und bietet keinen Anreiz zum Klicken. Er verrät nicht, was den Nutzer erwartet.

**Gutes Beispiel**  
„Steigern Sie Ihre Sichtbarkeit online mit professioneller SEO Beratung in Hamburg.  
Wir analysieren, optimieren und bringen Sie nach vorne! Jetzt kostenloses Erstgespräch vereinbaren.“  
→ Erklärung: Dieser Text ist prägnant, enthält Keywords (wenn auch nicht zwingend notwendig), bietet einen klaren Nutzen („Sichtbarkeit steigern“) und eine Handlungsaufforderung („kostenloses Erstgespräch“).
""")

    with st.expander("H1 (Hauptüberschrift)"):
        st.write("""
**Was es ist**  
Die H1 ist die wichtigste Überschrift auf Ihrer Webseite selbst.  
Sie sollte den Hauptinhalt der Seite zusammenfassen.

**Warum er wichtig ist**  
Suchmaschinen nutzen die H1, um das Hauptthema der Seite zu identifizieren.  
Sie ist auch für die Nutzer wichtig, um schnell zu erfassen, worum es auf der Seite geht.  
Pro Seite sollte es nur eine H1 geben.

**Schlechtes Beispiel**  
„Willkommen bei uns“ oder „Unser Angebot“  
→ Erklärung: Ähnlich wie beim Title sind diese Überschriften zu allgemein und geben keinen Aufschluss über den spezifischen Inhalt der Seite.

**Gutes Beispiel**  
„Professionelle SEO Beratung für Kleinunternehmen in Hamburg“  
→ Erklärung: Diese H1 ist spezifisch, enthält das Fokus-Keyword und gibt dem Nutzer sofort einen klaren Überblick über das Thema der Seite.  
Sie sollte das Haupt-Keyword der Seite enthalten.
""")

else:
    st.info("Bitte URLs laden und anschließend auf »Analyse starten« klicken.")
