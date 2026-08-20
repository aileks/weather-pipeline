"""The weather report UI: a read-only FastAPI app over the warehouse.

Routes and display formatting live here; all SQL and the connection policy
live in weather_pipeline.ui.queries. The contract is owned by
docs/reporting-ui.md: UTC-grain pages with city-local hour rendering, no
writes, no authentication, single local user.
"""

import calendar
import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from weather_pipeline.settings import Settings
from weather_pipeline.ui import queries
from weather_pipeline.ui.queries import WarehouseBusyError

BASE_DIR = Path(__file__).parent
EXPLORER_ROW_LIMIT = 500

WMO_DESCRIPTIONS = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "rime fog",
    51: "light drizzle",
    53: "drizzle",
    55: "dense drizzle",
    56: "freezing drizzle",
    57: "freezing drizzle",
    61: "slight rain",
    63: "rain",
    65: "heavy rain",
    66: "freezing rain",
    67: "freezing rain",
    71: "slight snow",
    73: "snow",
    75: "heavy snow",
    77: "snow grains",
    80: "rain showers",
    81: "rain showers",
    82: "violent rain showers",
    85: "snow showers",
    86: "snow showers",
    95: "thunderstorm",
    96: "thunderstorm with hail",
    99: "thunderstorm with heavy hail",
}

VARIABLE_LABELS = {
    "temperature_c": "temperature",
    "precipitation_mm": "precipitation",
    "wind_speed_kmh": "wind speed",
    "surface_pressure_hpa": "surface pressure",
}

# docs/anomaly-detection.md labels precipitation as a known-weak detector
WEAK_VARIABLES = frozenset({"precipitation_mm"})

COMPASS_POINTS = (
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
)


def compass_point(degrees: float | None) -> str:
    if degrees is None:
        return ""
    return COMPASS_POINTS[int((degrees + 11.25) % 360 // 22.5)]


def _num(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def _signed(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.1f}"


def wmo_label(code: int | None) -> str:
    return WMO_DESCRIPTIONS.get(code, "unknown code") if code is not None else ""


def _local_hour(hour_utc: dt.datetime, zone: ZoneInfo) -> dt.datetime:
    return hour_utc.replace(tzinfo=dt.UTC).astimezone(zone)


def _flag_view(flag: dict, zone: ZoneInfo) -> dict:
    hour_utc = flag["hour_ts_utc"]
    variable = flag["variable"]
    return {
        "location_id": flag.get("location_id", ""),
        "city_name": flag.get("city_name", ""),
        "hour_utc": hour_utc.strftime("%Y-%m-%d %H:%M"),
        "hour_local": _local_hour(hour_utc, zone).strftime("%Y-%m-%d %H:%M"),
        "hour_number": hour_utc.hour,
        "variable": variable,
        "variable_label": VARIABLE_LABELS.get(variable, variable),
        "weak": variable in WEAK_VARIABLES,
        "observed": flag["observed_value"],
        "baseline_mean": flag["baseline_mean"],
        "baseline_std": flag["baseline_std"],
        "comparable_obs_count": flag["comparable_obs_count"],
        "z_score": flag["z_score"],
    }


def _month_heat_class(count: int | None) -> str:
    if count is None:
        return "nodata"
    if count == 0:
        return "c0"
    if count <= 2:
        return "c1"
    if count <= 4:
        return "c2"
    if count <= 6:
        return "c3"
    return "c4"


def create_app(duckdb_path: Path | str | None = None) -> FastAPI:
    app = FastAPI(title="Weather reports", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.duckdb_path = (
        Path(duckdb_path) if duckdb_path is not None else Settings.from_env().duckdb_path
    )
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    templates.env.filters["num"] = _num
    templates.env.filters["signed"] = _signed
    app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

    @app.exception_handler(WarehouseBusyError)
    async def warehouse_busy(request: Request, exc: WarehouseBusyError):
        return templates.TemplateResponse(request, "error.html", {"kind": "busy"}, status_code=503)

    @app.exception_handler(duckdb.IOException)
    async def warehouse_unavailable(request: Request, exc: duckdb.IOException):
        return templates.TemplateResponse(
            request, "error.html", {"kind": "missing", "detail": str(exc)}, status_code=503
        )

    @app.get("/", response_class=HTMLResponse)
    def overview(request: Request):
        with queries.connect(app.state.duckdb_path) as conn:
            rows = queries.latest_summaries(conn)
        cards = []
        for row in rows:
            code = row["dominant_weather_code"]
            cards.append(
                {
                    "location_id": row["location_id"],
                    "city_name": row["city_name"],
                    "country": row["country"],
                    "timezone": row["timezone"],
                    "date_utc": row["date_utc"],
                    "temp_c_min": row["temp_c_min"],
                    "temp_c_max": row["temp_c_max"],
                    "temp_c_avg": row["temp_c_avg"],
                    "weather_label": wmo_label(code),
                    "weather_code": code,
                    "anomaly_count": row["anomaly_count"],
                }
            )
        return templates.TemplateResponse(request, "overview.html", {"cards": cards})

    @app.get("/locations/{location_id}", response_class=HTMLResponse)
    def location_redirect(request: Request, location_id: str):
        with queries.connect(app.state.duckdb_path) as conn:
            if queries.location(conn, location_id) is None:
                raise HTTPException(status_code=404, detail="Unknown location")
            latest = queries.latest_date(conn, location_id)
        target = latest if latest is not None else dt.datetime.now(dt.UTC).date()
        return RedirectResponse(f"/locations/{location_id}/{target.isoformat()}", status_code=303)

    @app.get("/locations/{location_id}/{date}", response_class=HTMLResponse)
    def daily_report(request: Request, location_id: str, date: str):
        try:
            day = dt.date.fromisoformat(date)
        except ValueError:
            raise HTTPException(status_code=404, detail="Unknown date") from None

        with queries.connect(app.state.duckdb_path) as conn:
            location = queries.location(conn, location_id)
            if location is None:
                raise HTTPException(status_code=404, detail="Unknown location")
            summary = queries.summary(conn, location_id, day)
            hour_rows = queries.hours(conn, location_id, day)
            flags = queries.day_anomalies(conn, location_id, day)

        zone = ZoneInfo(location["timezone"])
        hours = [
            {
                "utc": row["hour_ts_utc"].strftime("%H:%M"),
                "local": _local_hour(row["hour_ts_utc"], zone).strftime("%H:%M"),
                "temperature_c": row["temperature_c"],
                "apparent_temperature_c": row["apparent_temperature_c"],
                "precipitation_mm": row["precipitation_mm"],
                "relative_humidity_pct": row["relative_humidity_pct"],
                "wind_speed_kmh": row["wind_speed_kmh"],
                "wind_direction": compass_point(row["wind_direction_deg"]),
                "pressure_msl_hpa": row["pressure_msl_hpa"],
                "cloud_cover_pct": row["cloud_cover_pct"],
                "weather_code": row["weather_code"],
            }
            for row in hour_rows
        ]
        flag_views = [_flag_view(flag, zone) for flag in flags]

        flag_notes: dict[int, list[str]] = {}
        for flag in flag_views:
            flag_notes.setdefault(flag["hour_number"], []).append(
                f"{flag['variable_label']} z {flag['z_score']:+.1f}"
            )
        chart_payload = {
            "labels": [hour["utc"] for hour in hours],
            "localLabels": [hour["local"] for hour in hours],
            "temperature": [hour["temperature_c"] for hour in hours],
            "apparent": [hour["apparent_temperature_c"] for hour in hours],
            "flagHours": sorted(flag_notes),
            "flagNotes": flag_notes,
            "timezone": location["timezone"],
        }

        return templates.TemplateResponse(
            request,
            "daily.html",
            {
                "location": location,
                "date_utc": day,
                "prev_date": day - dt.timedelta(days=1),
                "next_date": day + dt.timedelta(days=1),
                "summary": summary,
                "weather_label": wmo_label(summary["dominant_weather_code"]) if summary else "",
                "dominant_wind_direction": compass_point(summary["dominant_wind_direction_deg"])
                if summary
                else "",
                "hours": hours,
                "flags": flag_views,
                "chart_payload": chart_payload,
                "variable_labels": VARIABLE_LABELS,
            },
        )

    @app.get("/anomalies", response_class=HTMLResponse)
    def explorer(
        request: Request,
        location: str | None = None,
        variable: str | None = None,
        date_from: dt.date | None = None,
        date_to: dt.date | None = None,
        z: str = "any",
    ):
        if variable is not None and variable not in VARIABLE_LABELS:
            raise HTTPException(status_code=422, detail="Unknown variable")
        if z not in ("any", "high", "low"):
            raise HTTPException(status_code=422, detail="z must be any, high, or low")

        with queries.connect(app.state.duckdb_path) as conn:
            all_locations = queries.locations(conn)
            rows, total = queries.explorer_anomalies(
                conn,
                location_id=location,
                variable=variable,
                date_from=date_from,
                date_to=date_to,
                z_sign=z,
                limit=EXPLORER_ROW_LIMIT,
            )
            span = queries.anomaly_span(conn)

        flags = [_flag_view(row, ZoneInfo(row["timezone"])) for row in rows]
        return templates.TemplateResponse(
            request,
            "anomalies.html",
            {
                "locations": all_locations,
                "variables": list(VARIABLE_LABELS),
                "variable_labels": VARIABLE_LABELS,
                "filters": {
                    "location": location or "",
                    "variable": variable or "",
                    "date_from": date_from.isoformat() if date_from else "",
                    "date_to": date_to.isoformat() if date_to else "",
                    "z": z,
                },
                "flags": flags,
                "total": total,
                "shown": len(flags),
                "limit": EXPLORER_ROW_LIMIT,
                "span": span,
            },
        )

    @app.get("/calendar", response_class=HTMLResponse)
    def calendar_page(request: Request, location: str | None = None, month: str | None = None):
        with queries.connect(app.state.duckdb_path) as conn:
            all_locations = queries.locations(conn)
            if not all_locations:
                raise HTTPException(status_code=404, detail="No locations in warehouse")
            chosen = next(
                (row for row in all_locations if row["location_id"] == location), all_locations[0]
            )
            latest = queries.latest_date(conn, chosen["location_id"])

        today = dt.datetime.now(dt.UTC).date()
        if month is None:
            anchor = latest or today
            year, month_number = anchor.year, anchor.month
        else:
            try:
                year, month_number = map(int, month.split("-"))
                dt.date(year, month_number, 1)
            except ValueError:
                raise HTTPException(
                    status_code=422, detail="month must look like 2026-08"
                ) from None
        year, month_number = min((year, month_number), (today.year, today.month))

        month_start = dt.date(year, month_number, 1)
        month_end = dt.date(year, month_number, calendar.monthrange(year, month_number)[1])
        with queries.connect(app.state.duckdb_path) as conn:
            counts = queries.month_counts(conn, chosen["location_id"], month_start, month_end)
        count_by_date = {row["date_utc"]: row["anomaly_count"] for row in counts}

        weeks = []
        for week in calendar.Calendar(firstweekday=0).monthdatescalendar(year, month_number):
            weeks.append(
                [
                    {
                        "date": day,
                        "in_month": day.month == month_number,
                        "count": count_by_date.get(day) if day <= today else None,
                        "heat_class": _month_heat_class(
                            count_by_date.get(day) if day <= today else None
                        ),
                        "is_today": day == today,
                    }
                    for day in week
                ]
            )

        previous_month = (month_start - dt.timedelta(days=1)).strftime("%Y-%m")
        next_month = None
        if month_end < today:
            next_month = (month_end + dt.timedelta(days=1)).strftime("%Y-%m")

        return templates.TemplateResponse(
            request,
            "calendar.html",
            {
                "locations": all_locations,
                "chosen": chosen,
                "year": year,
                "month_number": month_number,
                "month_name": calendar.month_name[month_number],
                "weeks": weeks,
                "previous_month": previous_month,
                "next_month": next_month,
            },
        )

    return app


app = create_app()
