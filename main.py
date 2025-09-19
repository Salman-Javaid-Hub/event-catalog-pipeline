# main.py
import os
import sys
import time
import subprocess
from pathlib import Path

# Ensure DB & tables exist before running anything else
from db import ensure_database, create_tables

PROJECT_ROOT = Path(__file__).parent.resolve()

# The steps you want to run in order
SCRIPTS = [
    ("Search (SerpAPI)", "search_scraper.py"),
    ("Parse (GPT extraction)", "parser.py"),
    ("Enrich (EIN + socials + sponsorship + fields)", "ein_enrichment.py"),
    ("Export to Google Sheets", "python export_to_sheets.py"),
]


def check_env():
    """Verify required environment variables."""
    missing = []
    if not os.getenv("OPENAI_API_KEY"):
        missing.append("OPENAI_API_KEY")

    # Warn (not fail) if SERPAPI_KEY is missing, since some runs may skip SerpAPI steps.
    if not os.getenv("SERPAPI_KEY"):
        print("⚠️  SERPAPI_KEY not set. If your search or EIN enrichment uses SerpAPI, set it.")

    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        print("\nExamples:")
        print("  • Windows PowerShell:  $env:OPENAI_API_KEY='sk-...'\n"
              "  • macOS/Linux bash:    export OPENAI_API_KEY='sk-...'\n")
        sys.exit(1)


def run_script_stream(title: str, filename: str):
    """Run a script and stream its output line-by-line with UTF-8 decoding."""
    script_path = PROJECT_ROOT / filename
    if not script_path.exists():
        print(f"❌ {title}: {filename} not found at {script_path}")
        sys.exit(1)

    print(f"\n▶️  {title} — {filename}")
    print(f"   Working dir: {PROJECT_ROOT}")
    start = time.time()

    env = os.environ.copy()
    # Ensure child uses UTF-8 for stdout/stderr
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    proc = subprocess.Popen(
        [sys.executable, "-u", str(script_path)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,                 # text mode
        encoding="utf-8",          # force utf-8 decode
        errors="replace",          # replace undecodable bytes
        bufsize=1,
        env=env,
    )

    try:
        for line in proc.stdout:
            print(line, end="")    # already newline-terminated
    except KeyboardInterrupt:
        proc.kill()
        print("\n⛔ Interrupted.")
        sys.exit(1)

    proc.wait()
    elapsed = time.time() - start

    if proc.returncode != 0:
        print(f"❌ {title} failed (exit code {proc.returncode}).")
        sys.exit(proc.returncode)

    print(f"✅ {title} completed in {elapsed:.1f}s")



def main():
    print("🚀 Event Catalog — Full Pipeline Run", flush=True)

    # 0) Env check
    check_env()

    # 1) Ensure DB + tables exist (safe to call every time)
    print("\n🧱 Ensuring database & tables…", flush=True)
    if not ensure_database():
        print("❌ Could not ensure database. Aborting.", flush=True)
        sys.exit(1)
    create_tables()

    # 2) Run each step in order with streaming logs
    for title, script in SCRIPTS:
        run_script_stream(title, script)

    print("\n🎉 All steps finished.", flush=True)


if __name__ == "__main__":
    main()
    