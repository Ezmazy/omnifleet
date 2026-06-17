# How to Upload OmniFleet to GitHub (step by step)

Written for someone doing this for the first time. The repository will be called
**omnifleet**. Follow it top to bottom.

> A note on wording: GitHub calls them "repositories" (repos). LinkedIn does not
> host code repos — you publish the project on GitHub, then **share the GitHub
> link in a LinkedIn post** so it shows on your profile. The last section covers
> that.

---

## Part 0 — One-time tool setup

1. **Make a GitHub account** at https://github.com if you do not have one.
2. **Install Git for Windows** from https://git-scm.com/download/win. Accept the
   defaults during install.
3. Open **PowerShell** and check it works:
   ```powershell
   git --version
   ```
   You should see a version number.
4. **Tell Git who you are** (use your GitHub email):
   ```powershell
   git config --global user.name  "Abdelrahman Elezmazy"
   git config --global user.email "your-github-email@example.com"
   ```

---

## Part 1 — Prepare the project folder

1. Put the documentation files I generated into your project root
   `D:\omnifleet_v003\` (next to `docker-compose.yml`):
   - `README.md`
   - `DOCUMENTATION.md`
   - `LICENSE`
   - `.gitignore`

2. **Important — remove data, secrets, and build junk before uploading.**
   The `.gitignore` already tells Git to skip them, but double-check these are
   NOT things you want public:
   - `notebooks/data/` (the big CSVs) — excluded by `.gitignore`
   - `spark/jars/*.jar` (large binaries) — excluded; people download them via the README
   - `dbt/target/`, `logs/`, `*.log`, `__pycache__/` — excluded
   - any real passwords — the compose file uses local demo passwords; that's fine
     for a student project, but never commit a real `.env`.

3. Open PowerShell in the project folder:
   ```powershell
   cd D:\omnifleet_v003
   ```

---

## Part 2 — Create the repository on GitHub (web)

1. Go to https://github.com and click the **+** (top right) → **New repository**.
2. Fill in:
   - **Repository name:** `omnifleet`
   - **Description:** `Scalable end-to-end fleet telematics data platform (Kafka, Spark, dbt, Airflow, Grafana, Superset).`
   - **Public** (so it shows on your profile and you can share it)
   - **Do NOT** tick "Add a README" / "Add .gitignore" / "Add license" — you
     already have those locally, and adding them here would cause a conflict.
3. Click **Create repository**. Leave the page open — you'll need the URL it
   shows (looks like `https://github.com/<your-username>/omnifleet.git`).

---

## Part 3 — Turn your folder into a Git repo and push

Run these in PowerShell, in `D:\omnifleet_v003`, one block at a time.

```powershell
# 1. start tracking this folder with Git
git init

# 2. (Git may default to "master"; rename the branch to "main")
git branch -M main

# 3. stage everything (the .gitignore automatically skips data/jars/logs)
git add .

# 4. see what WILL be uploaded - sanity check (no .csv, no .jar, no data/)
git status

# 5. make the first commit (a snapshot with a message)
git commit -m "Initial commit: OmniFleet fleet telematics data platform"

# 6. connect your local repo to the GitHub repo you created
#    (replace <your-username> with your real GitHub username)
git remote add origin https://github.com/<your-username>/omnifleet.git

# 7. upload
git push -u origin main
```

On the `git push`, a browser window or prompt asks you to **sign in to GitHub** —
do it once and Git remembers. If it asks for a password in the terminal, that
will not work with your normal password; see "Authentication" below.

When it finishes, refresh the GitHub page — your files and the README are live.

---

## Part 4 — Authentication (if the push asks for a password)

GitHub no longer accepts your account password on the command line. Two easy
options:

**Option A — sign in through the browser pop-up** (simplest). When the
"Connect to GitHub" window appears during `git push`, click **Sign in with your
browser** and authorize. Done.

**Option B — Personal Access Token (if no pop-up appears):**
1. GitHub → your avatar → **Settings** → **Developer settings** →
   **Personal access tokens** → **Tokens (classic)** → **Generate new token**.
2. Tick the **repo** scope, generate, and **copy the token** (you only see it once).
3. When `git push` asks for a password, **paste the token** instead of a password.

---

## Part 5 — Make the repo look professional

On the GitHub repo page:

1. **About** (right side, gear icon) → add:
   - Description (same as before)
   - **Topics** (tags): `data-engineering`, `apache-spark`, `apache-kafka`,
     `dbt`, `airflow`, `grafana`, `superset`, `postgresql`, `etl`, `medallion-architecture`
2. Confirm the **README renders** on the front page (it does automatically).
3. Optionally add a screenshot: drag a Grafana/Superset screenshot into the
   README in the GitHub web editor — it uploads and inserts the image link.

---

## Part 6 — Pushing future changes

Whenever you change files later:

```powershell
cd D:\omnifleet_v003
git add .
git commit -m "Describe what you changed"
git push
```

---

## Part 7 — Share it on LinkedIn

LinkedIn shows the project two ways. Do both.

### A) Add it to the "Projects" section of your profile
1. Go to your LinkedIn profile → **Add profile section** → **Recommended** →
   **Add projects**.
2. Fill in:
   - **Project name:** OmniFleet — End-to-End Fleet Telematics Data Platform
   - **Description:** short version of the README intro (problem + stack).
   - **Project URL:** your GitHub link `https://github.com/<your-username>/omnifleet`
   - Add your teammates as contributors.
3. Save.

### B) Write a post (this is what gets seen)
1. LinkedIn home → **Start a post**.
2. Paste something like:

   > 🚚 Excited to share **OmniFleet**, our ITI Data Engineering graduation
   > project: a scalable end-to-end fleet telematics data platform.
   >
   > It ingests live vehicle sensor data through Kafka, processes it with Spark
   > (real-time + batch), stores a medallion lakehouse on MinIO, builds a dbt
   > star schema in PostgreSQL, orchestrates daily incremental loads with Airflow
   > (watermark-based, idempotent), and serves a live Grafana incident map plus a
   > Superset/Power BI analytics dashboard.
   >
   > Highlights: real-time vs analytical decoupling, low-memory telemetry rollups,
   > exact cold-chain breach detection, and fully reproducible synthetic data.
   >
   > Code + full docs 👉 https://github.com/<your-username>/omnifleet
   >
   > Built with the team: Moayad Ehab, Mohamed Abdelnour, Mona Elgoba, Mohamed Osama.
   >
   > #DataEngineering #ApacheSpark #Kafka #dbt #Airflow #ETL

3. Add a screenshot or two (Grafana map, Superset dashboard, the architecture
   diagram) — posts with images get far more views.
4. **Tag your teammates** (type @ and their names) so it appears on their
   profiles too.
5. Post.

---

## Quick checklist

- [ ] Git installed, `user.name` / `user.email` set
- [ ] README.md, DOCUMENTATION.md, LICENSE, .gitignore in project root
- [ ] `git status` shows NO `.csv`, `.jar`, `data/`, or `logs/`
- [ ] Repo `omnifleet` created on GitHub (empty, public)
- [ ] `git push` succeeded; files visible on GitHub
- [ ] Topics + description added on the repo
- [ ] Added to LinkedIn Projects section
- [ ] LinkedIn post published with the GitHub link + teammates tagged
