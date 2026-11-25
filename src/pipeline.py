"""Ingestão e carga dimensional. Datas de corte em UTC, fim exclusivo."""
import argparse
import html
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from email.utils import parsedate_to_datetime

API = "https://api.spaceflightnewsapi.net/v4/articles/"
DB = Path(os.getenv("OBS_DB", "data/observatorio.db"))


def connect():
    DB.parent.mkdir(exist_ok=True, parents=True)
    db = sqlite3.connect(DB, timeout=90)
    db.execute("PRAGMA foreign_keys=ON")
    return db


def initialize(db):
    db.executescript(Path(__file__).with_name("schema.sql").read_text(encoding="utf8"))


def fetch(url):
    for attempt in range(6):
        try:
            req = Request(url, headers={"User-Agent": "Fucape-T2-Observatorio/1.0"})
            with urlopen(req, timeout=45) as response:
                return json.load(response)
        except (URLError, TimeoutError, ConnectionError) as exc:
            retry_after = 0
            if isinstance(exc, HTTPError):
                if exc.code not in (408, 429, 500, 502, 503, 504):
                    raise
                hint = exc.headers.get("Retry-After")
                if hint:
                    try:
                        retry_after = float(hint)
                    except ValueError:
                        retry_after = parsedate_to_datetime(hint).timestamp() - time.time()
            if attempt == 5:
                raise
            delay = max(retry_after, 2 ** attempt + random.random())
            print(f"Tentativa {attempt+1} falhou. Nova tentativa em {delay:.1f}s", flush=True)
            time.sleep(delay)


def ingest(start, end):
    if datetime.fromisoformat(start) >= datetime.fromisoformat(end):
        raise ValueError("O fim deve ser posterior ao início")
    url = API + "?" + urlencode(dict(limit=100, published_at_gte=start,
                                     published_at_lt=end, ordering="published_at"))
    seen = set()
    db = connect()
    try:
        initialize(db)
        # Área temporária privada. Nenhuma página parcial fica visível na bronze.
        db.execute("CREATE TEMP TABLE batch(id INTEGER PRIMARY KEY, payload TEXT)")
        while url:
            if url in seen or urlsplit(url)[:2] != urlsplit(API)[:2]:
                raise ValueError("Paginação repetida ou origem inesperada")
            seen.add(url)
            page = fetch(url)
            for item in page["results"]:
                payload = json.dumps(item, sort_keys=True, ensure_ascii=False)
                db.execute("INSERT OR REPLACE INTO batch VALUES(?,?)", (item["id"], payload))
            url = page["next"]
            if url:
                time.sleep(0.3)
        db.commit()
        with db:
            db.execute("""INSERT INTO raw(id,payload) SELECT id,payload FROM batch WHERE true
                          ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, dirty=1
                          WHERE raw.payload<>excluded.payload""")
            count = db.execute("SELECT count(*) FROM batch").fetchone()[0]
            db.execute("INSERT INTO audit(stage,detail) VALUES('bronze',?)",
                       (f"{start} até {end} (exclusivo), {count} artigos, {len(seen)} páginas",))
        print(f"Bronze confirmada: {count} artigos.")
    finally:
        db.close()


def clean(value):
    return " ".join(html.unescape(value or "").split())


def consolidate():
    db = connect()
    try:
        initialize(db)
        with db:
            db.execute("BEGIN IMMEDIATE")
            changed = 0
            for ident, raw in db.execute("SELECT id,payload FROM raw WHERE dirty=1"):
                item = json.loads(raw)
                date = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00")).astimezone(timezone.utc).date()
                day = date.isoformat()
                site = clean(item["news_site"]) or "Desconhecido"
                db.execute("INSERT OR IGNORE INTO dim_tempo(data,ano,mes,dia,trimestre,dia_iso) VALUES(?,?,?,?,?,?)",
                           (day,date.year,date.month,date.day,(date.month-1)//3+1,date.isoweekday()))
                db.execute("INSERT OR IGNORE INTO dim_veiculo(nome) VALUES(?)", (site,))
                sk, = db.execute("""INSERT INTO dim_artigo(id_fonte,titulo,resumo,url) VALUES(?,?,?,?)
                                    ON CONFLICT(id_fonte) DO UPDATE SET titulo=excluded.titulo,
                                    resumo=excluded.resumo,url=excluded.url RETURNING sk_artigo""",
                                 (ident,clean(item["title"]),clean(item["summary"]),item["url"])).fetchone()
                db.execute("""INSERT INTO fato_publicacao
                              SELECT ?, sk_tempo, sk_veiculo, 1, ? FROM dim_tempo, dim_veiculo
                              WHERE data=? AND nome=? ON CONFLICT(sk_artigo) DO UPDATE SET
                              sk_tempo=excluded.sk_tempo, sk_veiculo=excluded.sk_veiculo,
                              mencoes_lancamento=excluded.mencoes_lancamento""",
                           (sk,len({x["launch_id"] for x in item["launches"]}),day,site))
                changed += 1
            db.execute("UPDATE raw SET dirty=0 WHERE dirty=1")
            db.execute("INSERT INTO audit(stage,detail) VALUES('warehouse',?)",
                       (f"{changed} artigos processados",))
        print(f"DW confirmado: {changed} artigos processados.")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["ingest", "load"])
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2025-02-01")
    args = parser.parse_args()
    if args.action == "ingest":
        ingest(args.start, args.end)
    else:
        consolidate()
