"""REST API für die SEO-Analyse einzelner URLs."""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl, ValidationError

from crawler_async import crawl


app = FastAPI(
    title="SEO Snake API",
    description=(
        "Stellt einen Endpoint bereit, der eine URL entgegennimmt und die wichtigsten "
        "Analyse-Ergebnisse als JSON zurückliefert."
    ),
    version="1.0.0",
)


class AnalyzeRequest(BaseModel):
    """Request-Body für die Analyse eines Links."""

    url: HttpUrl


class AnalyzeResult(BaseModel):
    """Antwort mit kompakten SEO-Kennzahlen."""

    url: HttpUrl
    indexability_status: str
    robots_policy: str
    title: str
    meta_description: str
    h1: str
    word_count: int
    cms: str
    broken_links: List[str]


def _prepare_response(row: Dict[str, Any]) -> AnalyzeResult:
    """Mappt die Ausgabe des Crawlers auf das API-Schema."""

    broken_links = row.get("Broken Links")
    if not broken_links:
        broken_links_list: List[str] = []
    elif isinstance(broken_links, str):
        broken_links_list = [link.strip() for link in broken_links.split(",") if link.strip()]
    elif isinstance(broken_links, list):
        broken_links_list = [str(link) for link in broken_links if str(link).strip()]
    else:
        broken_links_list = [str(broken_links)]

    return AnalyzeResult(
        url=row.get("URL", ""),
        indexability_status=row.get("Status", ""),
        robots_policy=row.get("Robots Policy", ""),
        title=row.get("Title", ""),
        meta_description=row.get("Meta Description", ""),
        h1=row.get("H1", ""),
        word_count=int(row.get("Wörter", 0) or 0),
        cms=row.get("CMS", ""),
        broken_links=broken_links_list,
    )


@app.post("/analyze", response_model=AnalyzeResult, summary="Analysiert eine einzelne URL")
async def analyze_url(payload: AnalyzeRequest) -> AnalyzeResult:
    """Analysiert die übergebene URL und gibt 5-10 Kennzahlen zurück."""

    try:
        df = await crawl([str(payload.url)])
    except Exception as exc:  # pragma: no cover - Netzwerkfehler werden als HTTP-Fehler propagiert
        raise HTTPException(status_code=502, detail=f"Analyse fehlgeschlagen: {exc}") from exc

    if df.empty:
        raise HTTPException(status_code=500, detail="Keine Analyse-Daten verfügbar")

    row = df.iloc[0].to_dict()
    try:
        return _prepare_response(row)
    except ValidationError as exc:  # pragma: no cover - sollte nicht auftreten, aber absichern
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health", summary="Gesundheitscheck")
async def health() -> Dict[str, str]:
    """Einfacher Healthcheck-Endpunkt."""

    return {"status": "ok"}


if __name__ == "__main__":
    # Ermöglicht das lokale Starten mit `python api.py` ohne uvicorn direkt aufzurufen.
    import uvicorn

    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)
