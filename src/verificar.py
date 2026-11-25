"""Imprime a estrutura e verifica o grão e a integridade referencial."""
from contextlib import closing
from src.pipeline import connect


def main():
    with closing(connect()) as db:
        db.execute("BEGIN")
        for name, ddl in db.execute("""SELECT name,sql FROM sqlite_master WHERE type='table'
                                       AND (name LIKE 'dim_%' OR name LIKE 'fato_%') ORDER BY name"""):
            print(ddl)
        raw, pending = db.execute("SELECT count(*),coalesce(sum(dirty),0) FROM raw").fetchone()
        facts, qty = db.execute("SELECT count(*),coalesce(sum(quantidade),0) FROM fato_publicacao").fetchone()
        errors = db.execute("PRAGMA foreign_key_check").fetchall()
        print(f"Bronze={raw} | Pendentes={pending} | Fatos={facts} | Quantidade={qty} | Violações={len(errors)}")
        if errors or facts != qty or (not pending and raw != facts):
            raise SystemExit("Falha de integridade")
        for row in db.execute("SELECT * FROM audit ORDER BY run DESC LIMIT 6"):
            print(*row)


if __name__ == "__main__":
    main()

