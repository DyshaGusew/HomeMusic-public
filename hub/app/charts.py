"""«Чарты и жанры» (the Home screen): the charts of countries and genres, played in their order, shuffled or as a
wave (`app.mixes.wave`, seed `{"chart": id}`).

Sources (checked 2026-11 from a server outside Russia):
- Russia — Yandex Music's chart (100; `api.music.yandex.net` answers 451 outside Russia: the Russian proxy,
  `OUTBOUND_PROXY_HOSTS`), Apple Music's when Yandex fails. Deezer has no chart of Russia.
- USA, the European countries, Ukraine, the world — the «Top <country>» playlists of Deezer Charts (Deezer tracks as
  they are). «Европа» — the first places of its countries by turns.
- Genres — Deezer's chart of the genre (`chart/<genre>/tracks`; Deezer picks the country by the caller's address,
  the server's); Russian genres — Deezer playlists first, Last.fm tag top tracks as a fallback.
Names of Yandex and Apple are matched with the Deezer catalog the way mixes do it (`app.mixes.resolve.resolve_hits`:
only a sure match; a song the library has plays from it).

A chart is kept in `catalog_cache` (`chart:<id>`) and counts as fresh for `FRESH_S`: an older one is served while a new
one is made in the background, and `charts_loop` keeps them fresh so a tap finds one ready.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from itertools import zip_longest
from typing import Any

from app.catalog.cache import cache_get, cache_set
from app.catalog.models import CatalogTrack
from app.matching.normalize import fingerprint
from app.net import Priority, make_client, priority_context
from app.services import svc

log = logging.getLogger(__name__)

SIZE = 100
FRESH_S = 6 * 3600
KEEP_S = 7 * 86400  # an old chart is still served when its source is down
LOOP_START_S = 90
LOOP_EVERY_S = 3600
TAG_PLAYLISTS = 3  # editorial playlists of a Yandex tag merged into a Russian genre
APPLE_URL = "https://rss.marketingtools.apple.com/api/v2/{cc}/music/most-played/100/songs.json"


class ChartError(Exception):
    """The chart could not be made (its source is down or gave nothing)."""


@dataclass(frozen=True)
class Chart:
    id: str
    title: str  # the card's
    name: str  # the page's and the wave's («Волна: <name>»)
    kind: str  # "country" | "genre"
    group: str  # the Home screen's rows: "main", "europe", "ru_genres", "genres", "moods"
    source: str  # shown under the title
    deezer_playlist: int | None = None
    deezer_genre: int | None = None
    yandex_chart: str | None = None
    yandex_tag: str | None = None
    apple: str | None = None  # Apple Music's country: the chart when Yandex's fails
    parts: tuple[str, ...] = ()  # charts merged by turns («Европа»)
    dz_playlists: tuple[int, ...] = ()  # Deezer playlist fallbacks (return CatalogTrack directly)
    lastfm_tag: str = ""  # Last.fm tag fallback (names resolved via catalog)
    personal: bool = False  # built per user from their library/taste (not globally cached)


def _country(id_: str, title: str, name: str, playlist: int, group: str = "europe") -> Chart:
    return Chart(id_, title, name, "country", group, "Deezer", deezer_playlist=playlist)


def _genre(id_: str, title: str, genre: int) -> Chart:
    return Chart(id_, title, title, "genre", "genres", "Deezer", deezer_genre=genre)


def _ru_genre(id_: str, title: str, dz: tuple[int, ...] = (), lfm: str = "") -> Chart:
    return Chart(id_, title, title, "genre", "ru_genres", "Deezer", dz_playlists=dz, lastfm_tag=lfm)


def _mood(id_: str, title: str, lfm: str, keywords: tuple[str, ...] = ()) -> Chart:
    # `keywords` help match user's library artists by Last.fm tags to this mood.
    # Stored via a convention: we keep them in dz_playlists? no. Better encode in title? no.
    # We'll look them up by chart id in _MOOD_KEYWORDS.
    return Chart(id_, title, title, "genre", "moods", "Last.fm", lastfm_tag=lfm, personal=True)


# Mood keyword hints for matching user's library artists (Last.fm tags, case-insensitive substrings).
_MOOD_KEYWORDS: dict[str, tuple[str, ...]] = {
    "mood_calm": ("chill", "calm", "relax", "ambient", "mellow", "soft", "sleep", "dream", "acoustic", "instrumental"),
    "mood_energy": ("energetic", "energy", "upbeat", "dance", "party", "workout", "pump", "power", "hype"),
    "mood_sad": ("sad", "melanchol", "depress", "heartbreak", "lonely", "sorrow", "blue"),
    "mood_happy": ("happy", "feel good", "joy", "fun", "sunny", "uplifting", "positive"),
    "mood_romantic": ("romantic", "love", "sensual", "sexy", "intimate"),
    "mood_focus": ("focus", "study", "concentrat", "instrumental", "ambient", "lofi", "lo-fi", "classical", "piano"),
}


EUROPE = ("gb", "de", "fr", "it", "es", "pl", "nl")
CHARTS: dict[str, Chart] = {c.id: c for c in [
    Chart("ru", "Россия", "Чарт России", "country", "main", "Яндекс Музыка", yandex_chart="russia", apple="ru"),
    _country("us", "США", "Чарт США", 1313621735, "main"),
    Chart("eu", "Европа", "Чарт Европы", "country", "main", "Deezer", parts=EUROPE),
    _country("world", "Мир", "Мировой чарт", 3155776842, "main"),
    _country("gb", "Великобритания", "Чарт Великобритании", 1111142221),
    _country("de", "Германия", "Чарт Германии", 1111143121),
    _country("fr", "Франция", "Чарт Франции", 1109890291),
    _country("it", "Италия", "Чарт Италии", 1116187241),
    _country("es", "Испания", "Чарт Испании", 1116190041),
    _country("pl", "Польша", "Чарт Польши", 1266972311),
    _country("nl", "Нидерланды", "Чарт Нидерландов", 1266971851),
    _country("ua", "Украина", "Чарт Украины", 1362526495),
    _ru_genre("rusrock", "Русский рок", dz=(8100066862, 3459300842), lfm="russian rock"),
    _ru_genre("rusrap", "Русский рэп", dz=(6829975944,), lfm="russian hip hop"),
    _ru_genre("ruspop", "Русская поп-музыка", dz=(15559465163,), lfm="russian pop"),
    _ru_genre("phonk", "Фонк", dz=(10240601242,), lfm="phonk"),
    _mood("mood_calm", "Спокойное", "chill"),
    _mood("mood_energy", "Энергичное", "energetic"),
    _mood("mood_sad", "Грустное", "sad"),
    _mood("mood_happy", "Весёлое", "happy"),
    _mood("mood_romantic", "Романтичное", "romantic"),
    _mood("mood_focus", "Для фокуса", "focus"),
    _genre("pop", "Поп", 132),
    _genre("hiphop", "Хип-хоп", 116),
    _genre("rock", "Рок", 152),
    _genre("dance", "Танцевальная", 113),
    _genre("electro", "Электроника", 106),
    _genre("rnb", "R&B", 165),
    _genre("alternative", "Альтернатива", 85),
    _genre("metal", "Метал", 464),
    _genre("jazz", "Джаз", 129),
    _genre("classical", "Классика", 98),
    _genre("soul", "Соул и фанк", 169),
    _genre("blues", "Блюз", 153),
    _genre("latin", "Латино", 197),
    _genre("reggae", "Регги", 144),
    _genre("folk", "Фолк", 466),
    _genre("soundtrack", "Саундтреки", 173),
    _genre("asian", "K-pop и Азия", 16),
]}


@dataclass
class Built:
    tracks: list[CatalogTrack]
    at: float  # Unix time it was made
    positions: list[int] = field(default_factory=list)  # served: each track's place in the chart (1-based)


_locks: dict[str, asyncio.Lock] = {}
_later: dict[str, asyncio.Task] = {}


def get(chart_id: str) -> Chart | None:
    return CHARTS.get(chart_id)


def _key(chart_id: str) -> str:
    return f"chart:{chart_id}"


def _fresh(b: Built) -> bool:
    return time.time() - b.at <= FRESH_S


async def cached(chart_id: str) -> Built | None:
    """The chart as kept, however old."""
    d = await cache_get(_key(chart_id))
    if not d:
        return None
    try:
        return Built([CatalogTrack.model_validate(t) for t in d["tracks"]], float(d["at"]))
    except Exception as e:
        log.warning("chart %s: unreadable: %r", chart_id, e)
        return None


async def tracks(chart_id: str, user_id: int | None = None) -> Built:
    """The chart with its tracks' places. Made now when none is kept (it may take a minute: Yandex's
    names are matched with the catalog); an old one is served while a new one is made in the background. Raises
    ChartError when it can't be made. Personal charts are built per user (not globally cached)."""
    c = CHARTS.get(chart_id)
    if c is None:
        raise ChartError("такого чарта нет")
    if c.personal:
        if user_id is None:
            raise ChartError("нужен пользователь")
        return await personal_tracks(chart_id, user_id)
    built = await cached(chart_id)
    if built is None:
        built = await refresh(chart_id)
    elif not _fresh(built):
        _refresh_later(chart_id)
    return Built(list(built.tracks), built.at, list(range(1, len(built.tracks) + 1)))


async def refresh(chart_id: str) -> Built:
    """Makes the chart anew unless another caller just did."""
    async with _locks.setdefault(chart_id, asyncio.Lock()):
        built = await cached(chart_id)
        if built is not None and _fresh(built):
            return built
        found = await _make(CHARTS[chart_id])
        if not found:
            raise ChartError("источник ничего не вернул")
        built = Built(found, time.time())
        await cache_set(_key(chart_id), {"at": built.at, "tracks": [t.model_dump(mode="json") for t in found]}, KEEP_S)
        log.info("chart %s: %s tracks", chart_id, len(found))
        return built


def _background(coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
    """Below the user's requests on the network (`app.net.Priority`)."""
    return asyncio.create_task(coro, context=priority_context(Priority(True)))


def _refresh_later(chart_id: str) -> None:
    if (t := _later.get(chart_id)) is not None and not t.done():
        return
    task = _later[chart_id] = _background(refresh(chart_id))

    def done(t: asyncio.Task) -> None:
        if not t.cancelled() and (e := t.exception()):
            log.warning("chart %s: refresh failed: %r", chart_id, e)

    task.add_done_callback(done)


async def charts_loop() -> None:
    """Keeps the charts fresh in the background, so a tap finds one ready: the first round a minute and a half after the
    start, then every hour the ones older than `FRESH_S`."""
    await asyncio.sleep(LOOP_START_S)
    while True:
        for cid in CHARTS:
            try:
                built = await cached(cid)
                if built is None or not _fresh(built):
                    await _background(refresh(cid))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("chart %s: %r", cid, e)
        await asyncio.sleep(LOOP_EVERY_S)


async def reset() -> None:
    tasks = [t for t in _later.values() if not t.done()]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    _later.clear()
    _locks.clear()


# --- the sources -------------------------------------------------------------------------------------------------


async def _make(c: Chart) -> list[CatalogTrack]:
    if c.parts:
        lists = await asyncio.gather(*(tracks(p) for p in c.parts), return_exceptions=True)
        ok = [b.tracks for b in lists if isinstance(b, Built)]
        if not ok:
            raise ChartError(f"чарты стран недоступны: {lists[0]!r}")
        return by_turns(ok)
    if c.deezer_playlist is not None:
        return await _deezer(lambda: svc.deezer.playlist_tracks(c.deezer_playlist, SIZE))
    if c.deezer_genre is not None:
        return await _deezer(lambda: svc.deezer.chart_tracks(c.deezer_genre, SIZE))
    if c.yandex_chart:
        try:
            if found := await _resolve("yandex", await asyncio.to_thread(_yandex_chart_sync, c.yandex_chart)):
                return found
            log.warning("chart %s: Yandex gave nothing, Apple Music's instead", c.id)
        except Exception as e:
            if not c.apple:
                raise ChartError(f"Яндекс Музыка: {e!r}") from e
            log.warning("chart %s: Yandex failed (%r), Apple Music's instead", c.id, e)
    if c.yandex_tag:
        yerr: Exception | None = None
        try:
            if found := await _resolve("yandex", await asyncio.to_thread(_yandex_tag_sync, c.yandex_tag)):
                return found
            log.warning("chart %s: Yandex gave nothing, trying fallbacks", c.id)
            yerr = ChartError("Яндекс Музыка: пустой ответ")
        except Exception as e:
            yerr = e
            log.warning("chart %s: Yandex failed (%r), trying fallbacks", c.id, e)
        # fallback 1: Deezer playlists (already CatalogTrack, resolve not needed)
        if c.dz_playlists and svc.deezer is not None:
            tracks_out: list[CatalogTrack] = []
            for pid in c.dz_playlists:
                try:
                    tracks_out += await svc.deezer.playlist_tracks(pid, limit=SIZE)
                except Exception as e:
                    log.warning("chart %s: Deezer playlist %s failed: %r", c.id, pid, e)
            tracks_out = _dedupe_tracks(tracks_out)
            if tracks_out:
                return tracks_out[:SIZE]
        # fallback 2: Last.fm tag (names -> resolve)
        if c.lastfm_tag:
            try:
                hits = await _lastfm_tag(c.lastfm_tag)
                if hits:
                    found = await _resolve("lastfm", hits)
                    if found:
                        return found
            except Exception as e:
                log.warning("chart %s: Last.fm tag %r failed: %r", c.id, c.lastfm_tag, e)
        raise ChartError(f"Яндекс Музыка: {yerr!r}") from yerr
    if c.dz_playlists or c.lastfm_tag:
        # Deezer playlists first (CatalogTrack), Last.fm tag names as fallback.
        if c.dz_playlists and svc.deezer is not None:
            tracks_out: list[CatalogTrack] = []
            for pid in c.dz_playlists:
                try:
                    tracks_out += await svc.deezer.playlist_tracks(pid, limit=SIZE)
                except Exception as e:
                    log.warning("chart %s: Deezer playlist %s failed: %r", c.id, pid, e)
            tracks_out = _dedupe_tracks(tracks_out)
            if tracks_out:
                return tracks_out[:SIZE]
        if c.lastfm_tag:
            try:
                hits = await _lastfm_tag(c.lastfm_tag)
                if hits:
                    found = await _resolve("lastfm", hits)
                    if found:
                        return found
            except Exception as e:
                log.warning("chart %s: Last.fm tag %r failed: %r", c.id, c.lastfm_tag, e)
        raise ChartError("источник ничего не вернул")
    if c.apple:
        try:
            return await _resolve("apple", await _apple(c.apple))
        except Exception as e:
            raise ChartError(f"Apple Music: {e!r}") from e
    raise ChartError("у чарта нет источника")


def by_turns(lists: list[list[CatalogTrack]], size: int = SIZE) -> list[CatalogTrack]:
    """The first places of every list, then the second ones…; a song once (by ref and by artist + title)."""
    out: list[CatalogTrack] = []
    seen: set[str] = set()
    for row in zip_longest(*lists):
        for t in row:
            if t is None:
                continue
            keys = {t.ref, fingerprint(t.artist, t.title)}
            if keys & seen:
                continue
            seen |= keys
            out.append(t)
            if len(out) >= size:
                return out
    return out


def _dedupe_tracks(tracks: list[CatalogTrack], size: int = SIZE) -> list[CatalogTrack]:
    """Keep first occurrence of each track by ref and by artist + title."""
    out: list[CatalogTrack] = []
    seen: set[str] = set()
    for t in tracks:
        keys = {t.ref, fingerprint(t.artist, t.title)}
        if keys & seen:
            continue
        seen |= keys
        out.append(t)
        if len(out) >= size:
            return out
    return out


async def _deezer(fetch: Callable[[], Awaitable[list[CatalogTrack]]]) -> list[CatalogTrack]:
    if svc.deezer is None:
        raise ChartError("Deezer не настроен")
    try:
        return (await fetch())[:SIZE]
    except Exception as e:
        raise ChartError(f"Deezer: {e!r}") from e


async def _resolve(source: str, hits: list) -> list[CatalogTrack]:
    """The names' catalog tracks in order; the ones not found for sure are left out."""
    from app.mixes.resolve import resolve_hits

    out: list[CatalogTrack] = []
    seen: set[str] = set()
    for t in await resolve_hits(source, hits):
        if t is not None and t.ref not in seen:
            seen.add(t.ref)
            out.append(t)
    return out[:SIZE]


def _library_to_catalog(t) -> CatalogTrack:
    """A library DB track as a CatalogTrack (prefers Deezer ref when known, else nd)."""
    artist = t.artist or ""
    title = t.title or ""
    if t.deezer_track_id:
        return CatalogTrack(ref=f"dz:{t.deezer_track_id}", source="dz", id=str(t.deezer_track_id),
                            title=title, artist=artist, artists=[artist] if artist else [],
                            album_title=t.album, duration_s=t.duration_s, isrc=t.isrc, explicit=t.explicit)
    return CatalogTrack(ref=f"nd:{t.nd_song_id}", source="nd", id=str(t.nd_song_id or t.id),
                        title=title, artist=artist, artists=[artist] if artist else [],
                        album_title=t.album, duration_s=t.duration_s, isrc=t.isrc, explicit=t.explicit)


async def _personal_library_tracks(chart_id: str, user_id: int, limit: int = 50) -> list[CatalogTrack]:
    """User's library tracks whose artists' Last.fm tags match the mood (no GPU)."""
    from sqlalchemy import desc, func, or_, select

    from app.db import session_scope
    from app.mixes.sources import lastfm_artist_tags, lastfm_configured
    from app.models import PlayEvent, Track

    keywords = _MOOD_KEYWORDS.get(chart_id) or ()
    mood_word = (CHARTS[chart_id].lastfm_tag or chart_id).casefold()

    async with session_scope() as s:
        # Tracks the user interacted with in the library (by Navidrome song id).
        played_ids = (
            select(PlayEvent.nd_song_id)
            .where(
                PlayEvent.user_id == user_id,
                PlayEvent.event.in_(("finish", "like", "start")),
                PlayEvent.nd_song_id.is_not(None),
            )
            .distinct()
            .subquery()
        )
        q_played = (
            select(Track.artist, func.count(Track.id).label("c"))
            .where(Track.nd_song_id.in_(select(played_ids.c.nd_song_id)))
            .group_by(Track.artist)
            .order_by(desc("c"))
            .limit(40)
        )
        artists = [r[0] for r in (await s.execute(q_played)).all() if r[0]]

        # Also consider artists from liked/played catalog refs that map to library tracks.
        if len(artists) < 15:
            q_ref = (
                select(Track.artist, func.count(PlayEvent.id).label("c"))
                .select_from(PlayEvent)
                .join(Track, func.replace(PlayEvent.ref, "dz:", "") == Track.deezer_track_id)
                .where(
                    PlayEvent.user_id == user_id,
                    PlayEvent.event.in_(("finish", "like", "start")),
                    PlayEvent.ref.like("dz:%"),
                    Track.deezer_track_id.is_not(None),
                )
                .group_by(Track.artist)
                .order_by(desc("c"))
                .limit(30)
            )
            for a, _ in (await s.execute(q_ref)).all():
                if a and a not in artists:
                    artists.append(a)

        # Fallback: any artists present in the library.
        if not artists:
            r = await s.execute(select(Track.artist).distinct().limit(40))
            artists = [x[0] for x in r.all() if x[0]]

        matched_artists: list[str] = []
        if lastfm_configured() and (keywords or mood_word):
            for a in artists[:30]:
                try:
                    tags = [t.casefold() for t in await lastfm_artist_tags(a)]
                except Exception:
                    tags = []
                blob = " ".join(tags)
                if any(k in blob for k in keywords) or (mood_word and mood_word in blob):
                    matched_artists.append(a)
        if not matched_artists:
            # Keep it personal even without tag matches: prefer the user's active artists.
            matched_artists = list(dict.fromkeys(artists[:20]))

        # Fetch user's own played library tracks first (best "my tracks"), then others by matched artists.
        played_rows = (
            await s.execute(
                select(Track)
                .where(Track.nd_song_id.in_(select(played_ids.c.nd_song_id)))
                .order_by(desc(Track.id))
                .limit(400)
            )
        ).scalars().all()

        other_rows = []
        if matched_artists:
            other_rows = (
                await s.execute(
                    select(Track)
                    .where(Track.artist.in_(matched_artists))
                    .order_by(desc(Track.id))
                    .limit(400)
                )
            ).scalars().all()

    # Prefer played tracks whose artists match mood; otherwise include played tracks and matched-artist tracks.
    played_set = {t.id for t in played_rows}
    matched_set = set(matched_artists)
    candidates = []
    # 1) played tracks by mood-matched artists
    candidates += [t for t in played_rows if t.artist in matched_set]
    # 2) other played tracks (still "my tracks")
    candidates += [t for t in played_rows if t.artist not in matched_set]
    # 3) library tracks by matched artists not yet included
    candidates += [t for t in other_rows if t.id not in played_set]

    seen: set[str] = set()
    out: list[CatalogTrack] = []
    for t in candidates:
        key = fingerprint(t.artist or "", t.title or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(_library_to_catalog(t))
        if len(out) >= limit:
            break
    return out


async def _personal_similar_catalog(mine: list[CatalogTrack], limit: int = 30) -> list[CatalogTrack]:
    """Catalog tracks similar to user's own mood picks (helps keep recommendations close to user's taste/language)."""
    from app.mixes.sources import lastfm_configured, lastfm_similar_tracks

    if not mine or not lastfm_configured():
        return []
    seeds = []
    seen_seed: set[str] = set()
    for t in mine:
        key = fingerprint(t.artist or "", t.title or "")
        if not key or key in seen_seed or not t.artist or not t.title:
            continue
        seen_seed.add(key)
        seeds.append((t.artist, t.title))
        if len(seeds) >= 3:
            break
    out: list[CatalogTrack] = []
    seen: set[str] = set()
    for artist, title in seeds:
        try:
            hits = await lastfm_similar_tracks(artist, title, limit=15)
            if not hits:
                continue
            found = await _resolve("lastfm", hits)
            for t in found:
                if t.ref in seen:
                    continue
                seen.add(t.ref)
                out.append(t)
                if len(out) >= limit:
                    return out
        except Exception:
            continue
    return out


async def personal_tracks(chart_id: str, user_id: int) -> Built:
    """Per-user mood chart: mix of user's library tracks and catalog tracks (no GPU)."""
    c = CHARTS[chart_id]
    catalog: list[CatalogTrack] = []
    # Catalog/mood candidates from Last.fm tag (and optional Deezer playlists).
    if c.dz_playlists and svc.deezer is not None:
        tracks_out: list[CatalogTrack] = []
        for pid in c.dz_playlists:
            try:
                tracks_out += await svc.deezer.playlist_tracks(pid, limit=SIZE)
            except Exception as e:
                log.warning("chart %s: Deezer playlist %s failed: %r", c.id, pid, e)
        catalog = _dedupe_tracks(tracks_out)
    if not catalog and c.lastfm_tag:
        try:
            hits = await _lastfm_tag(c.lastfm_tag)
            if hits:
                catalog = await _resolve("lastfm", hits)
        except Exception as e:
            log.warning("chart %s: Last.fm mood tag %r failed: %r", c.id, c.lastfm_tag, e)

    try:
        mine = await _personal_library_tracks(chart_id, user_id)
    except Exception as e:
        log.warning("chart %s: personal library tracks failed: %r", chart_id, e)
        mine = []

    # Catalog similar to user's own picks keeps recommendations closer to the user's taste/language.
    try:
        similar = await _personal_similar_catalog(mine)
    except Exception as e:
        log.warning("chart %s: personal similar catalog failed: %r", chart_id, e)
        similar = []

    # Interleave mine and catalog/similar so the list contains both.
    pools = [p for p in [mine, similar, catalog] if p]
    mixed = by_turns(pools) if pools else []
    if not mixed:
        raise ChartError("не удалось подобрать треки по настроению")
    mixed = mixed[:SIZE]
    return Built(mixed, time.time(), list(range(1, len(mixed) + 1)))


def _yandex_chart_sync(name: str) -> list:
    from app.catalog.yandex import yandex_client
    from app.mixes.sources import _yandex_rows

    info = yandex_client().chart(name)
    rows = [x.track for x in (info.chart.tracks or [])] if info is not None and info.chart is not None else []
    return _yandex_rows([t for t in rows if t is not None])[:SIZE]


def _yandex_tag_sync(tag: str) -> list:
    from app.catalog.yandex import yandex_client
    from app.mixes.sources import _yandex_rows

    client = yandex_client()
    found = client.tags(tag)
    lists = []
    for pid in (found.ids or [])[:TAG_PLAYLISTS] if found is not None else []:
        pl = client.users_playlists(pid.kind, pid.uid)
        rows = [ts.track for ts in (getattr(pl, "tracks", None) or []) if ts.track is not None]
        lists.append(_yandex_rows(rows))
    out, seen = [], set()
    for row in zip_longest(*lists):
        for h in row:
            if h is not None and (k := (h.artist.casefold(), h.title.casefold())) not in seen:
                seen.add(k)
                out.append(h)
    return out[:SIZE]


async def _apple(country: str) -> list:
    from app.mixes.sources import Hit

    client = make_client(timeout=20)
    try:
        r = await client.get(APPLE_URL.format(cc=country))
        r.raise_for_status()
        rows = r.json()["feed"]["results"]
    finally:
        await client.aclose()
    return [Hit(artist=x.get("artistName") or "", title=x.get("name") or "", source_id=f"apple:{x.get('id')}")
            for x in rows]


async def _lastfm_tag(tag: str, limit: int = SIZE) -> list:
    """Top tracks for a Last.fm tag as Hits (names to be resolved against catalog)."""
    from app.mixes.sources import Hit, _as_list, _lastfm, lastfm_configured

    if not lastfm_configured():
        return []
    body = await _lastfm("tag.getTopTracks", tag=tag, limit=limit)
    hits: list[Hit] = []
    for t in _as_list((body.get("tracks") or {}).get("track")):
        artist = t.get("artist")
        if isinstance(artist, dict):
            artist = artist.get("name") or ""
        title = t.get("name") or ""
        if artist and title:
            hits.append(Hit(artist=artist, title=title))
    return hits[:limit]
