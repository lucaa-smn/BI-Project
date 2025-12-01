from pathlib import Path
from config import get_engine
from sqlalchemy.exc import SQLAlchemyError


def get_schema_path() -> Path:
    etl_dir = Path(__file__).resolve().parent
    project_root = etl_dir.parent
    schema_path = project_root / "dwh" / "schema.sql"
    return schema_path


def run_schema():
    schema_path = get_schema_path()

    if not schema_path.exists():
        raise FileNotFoundError(
            f"schema.sql nicht gefunden unter: {schema_path} "
            f"(prüfe Pfad und Dateiname)"
        )

    print(f"Verwende schema.sql unter: {schema_path}")

    schema_sql = schema_path.read_text(encoding="utf-8")

    engine = get_engine()

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(schema_sql)

        print("✅ Schema erfolgreich in der Datenbank angelegt.")

    except SQLAlchemyError as e:
        print("❌ Fehler beim Ausführen des Schemas:")
        print(e)
        raise


if __name__ == "__main__":
    run_schema()
