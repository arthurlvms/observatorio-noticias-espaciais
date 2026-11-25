"""Painel HTML servido por Python. Consultas e benchmark sobre o mesmo snapshot."""
import os
from contextlib import closing
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from src.pipeline import connect, initialize
from src.search import read_index


def bars(rows):
    maximum = max((n for _, n in rows), default=1)
    return "".join(f"<div class='bar'><span>{escape(label)}</span>"
                   f"<meter min='0' max='{maximum}' value='{n}'></meter><b>{n}</b></div>"
                   for label, n in rows)


def page(query):
    with closing(connect()) as db:
        db.execute("BEGIN")
        total, sites, launches = db.execute("""SELECT coalesce(sum(quantidade),0),
            count(DISTINCT sk_veiculo),coalesce(sum(mencoes_lancamento),0) FROM fato_publicacao""").fetchone()
        months = db.execute("""SELECT substr(data,1,7),sum(quantidade) FROM fato_publicacao
            JOIN dim_tempo USING(sk_tempo) GROUP BY 1 ORDER BY 1""").fetchall()
        outlets = db.execute("""SELECT nome,sum(quantidade) FROM fato_publicacao
            JOIN dim_veiculo USING(sk_veiculo) GROUP BY 1 ORDER BY 2 DESC,1""").fetchall()
        raw, pending = db.execute("SELECT count(*),coalesce(sum(dirty),0) FROM raw").fetchone()
        orphans = len(db.execute("PRAGMA foreign_key_check").fetchall())
        audit = db.execute("SELECT stage,detail,finished_utc FROM audit ORDER BY run DESC LIMIT 6").fetchall()
        index = read_index(db)
    fixed, packed = index.sizes
    bench = results = ""
    if query and total:
        before, after, delta, speed = index.benchmark(query)
        bench = (f"<p>Antes <strong>{before:.5f} ms</strong> · Depois <strong>{after:.5f} ms</strong>"
                 f" · Δ {delta:+.1f}% · Speedup {speed:.2f}×</p><small>Média de 40 execuções"
                 " por método, com alternância de ordem e verificação do top 10.</small>")
        for doc, score in index.search(query, True):
            title, summary, url = index.docs[doc]
            link = escape(url, quote=True) if urlsplit(url).scheme in {"http","https"} else "#"
            results += (f"<article><small>Pontuação {score:.2f}</small><h3><a href='{link}'>"
                        f"{escape(title)}</a></h3><p>{escape(summary)}</p></article>")
        if not results:
            results = "<p>Nenhuma matéria contém os termos consultados.</p>"
    logs = "".join(f"<li>{escape(ts)} UTC · {escape(stage)} · {escape(detail)}</li>"
                   for stage, detail, ts in audit)
    state = ("Aguardando a primeira carga." if not total else
             f"Acervo de {months[0][0]} a {months[-1][0]}.")
    return f"""<!doctype html><html lang="pt-BR"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Observatório espacial</title>
<style>*{{box-sizing:border-box}}body{{max-width:1050px;margin:auto;padding:36px 5%;background:#f4f4ed;
color:#102e32;font:17px/1.55 'Segoe UI',sans-serif}}h1,h2,h3,p{{margin:.3em 0}}
h1{{font:clamp(38px,7vw,68px)/1.05 Georgia,serif}}h2{{font:32px Georgia,serif}}
header,section{{padding:26px 0;border-bottom:1px solid #b4c3bc}}.grid{{display:grid;
grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:20px}}strong{{color:#02665d}}
.kpi strong{{display:block;font-size:36px}}.bar{{display:grid;align-items:center;
grid-template-columns: minmax(90px,1fr) 2fr 40px;gap:8px}}meter{{width:100%}}
a{{color:#125f60}}input,button{{padding:10px;font:inherit}}input{{width:min(65%,500px)}}
button{{background:#125f60;color:white;border:2px solid #125f60}}article{{margin:18px 0}}
small{{color:#405b57}}nav a{{margin-right:16px}}</style>
<header><h1>Observatório espacial</h1><p>{state}</p><nav><a href="#cobertura">Cobertura</a>
<a href="#busca">Busca</a><a href="#engenharia">Engenharia</a></nav></header><main>
<section class="grid kpi"><div><strong>{total}</strong>matérias</div><div><strong>{sites}</strong>
veículos</div><div><strong>{launches}</strong>vínculos com lançamentos</div></section>
<section id="cobertura"><h2>Cobertura editorial</h2><div class="grid"><div><h3>Matérias por veículo</h3>
{bars(outlets)}</div><div><h3>Matérias por mês</h3>{bars(months)}</div></div></section>
<section id="busca"><h2>Buscar no acervo</h2><form action="/#busca"><label for="q">Termos em títulos
e resumos</label><br><input id="q" name="q" value="{escape(query, quote=True)}" placeholder="lunar lander">
<button>Buscar</button></form>{results}</section><section id="engenharia"><h2>Engenharia e benchmark</h2>
{bench or '<p>Pesquise um termo para medir o tempo de consulta.</p>'}
<p>Listas de postagens · uint32 {fixed/1e6:.6f} MB · VB {packed/1e6:.6f} MB · economia
{100*(1-packed/fixed) if fixed else 0:.2f}%</p><p>Bronze {raw} · Fatos {total} · Pendentes {pending}
 · Violações de chave estrangeira {orphans}</p><h3>Últimas cargas confirmadas</h3><ul>{logs}</ul>
</section></main></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        if target.path != "/":
            self.send_error(404)
            return
        query = parse_qs(target.query).get("q", [""])[0][:250]
        body = page(query).encode("utf8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    with closing(connect()) as connection:
        initialize(connection)
    ThreadingHTTPServer((os.getenv("OBS_HOST", "127.0.0.1"), 8501), Handler).serve_forever()

