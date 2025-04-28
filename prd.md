# Product Requirements Document: EV Betting Backend Service (MVP)

## 1. Introduction / Overview

This document outlines the requirements for the Minimum Viable Product (MVP) of a backend service designed to identify Expected Value (EV) betting opportunities. The system will scrape odds data from multiple specified online sportsbooks (including public and hidden APIs), normalize the disparate JSON data structures into a unified format, compare odds against a designated sharp reference book to calculate EV and Kelly Criterion stake suggestions, and store the results in a Supabase database for querying and analysis. The initial focus is on providing a reliable automated data pipeline for personal use.

## 2. Goals / Objectives

* **Automate Data Aggregation:** Reliably scrape odds data for specified sports and markets from at least 3 sportsbooks (including Crab Sports and Pinnacle) every minute.
* **Standardize Data:** Normalize inconsistent data structures from different sources into a unified schema for games, markets, and odds.
* **Identify Value Bets:** Accurately calculate vig-free probabilities from a designated sharp book (Pinnacle for MVP) and identify positive EV betting opportunities against other target books (Crab Sports for MVP).
* **Guide Staking:** Calculate the Kelly Criterion fraction for all positive EV bets to provide suggested stake sizing.
* **Enable Analysis:** Store normalized odds, calculations, and related metadata in a structured Supabase database, enabling efficient querying and filtering.
* **Develop MVP Rapidly:** Deliver a functional MVP meeting these core objectives within a short timeframe (approx. 1 week).

## 3. Target Audience / User Personas

* **Primary User (MVP):** The developer (yourself) needing an automated tool for personal sports betting analysis and EV opportunity identification.
* **Potential Future Users:** Small private betting groups, other individual sophisticated bettors.

## 4. User Stories / Use Cases (MVP)

* **As the system user, I want to** automatically scrape odds data for NBA, NHL, and MLB (Moneyline, Spread, Total) from Crab Sports, Pinnacle, and [one other specified book TBD] every minute, **so that** I have near real-time odds available for comparison.
* **As the system user, I want to** have the scraped odds automatically normalized into a standard structure (identifying the same games and markets across books), **so that** I can accurately compare odds for the same betting opportunities.
* **As the system user, I want the system to** automatically calculate the vig-free probability from Pinnacle odds and compare it against Crab Sports odds to find the Expected Value (EV%) for each market, **so that** I can identify potentially profitable bets.
* **As the system user, I want the system to** calculate the Kelly Criterion stake percentage for all positive EV bets found, **so that** I have a guideline for bet sizing based on perceived edge and odds.
* **As the system user, I want to** have all normalized odds, calculated EV%, and Kelly stakes stored in a Supabase database, **so that** I can query, filter, and analyze the betting opportunities historically.
* **As the system user, I want to** be able to query the Supabase database (e.g., via a simple script or CLI tool) to view the top positive EV bets, sorted by EV% or Kelly fraction, filtered by sport or market type, **so that** I can quickly find the most promising bets available right now.
* **As the system user, I want** failed scrapes (after retries) to be logged with details, **so that** I can diagnose and fix issues with specific book scrapers or configurations (like expired cookies).

## 5. Functional Requirements

### 5.1. Scraping

* Scrape odds data from specified sportsbooks:
  * Pinnacle (Sharp Reference for MVP)
  * Crab Sports (Target Book for MVP)
  * [One additional sportsbook TBD] (Target Book for MVP)
* Support scraping for specific sports: NBA, NHL, MLB.
* Support scraping for specific primary market types: Moneyline, Spread (including Run Line, Puck Line), Total (Over/Under).
* Execute scraping process on a schedule (every 1 minute).
* Handle different API types: public, hidden APIs requiring headers/cookies, APIs requiring static keys.
* Implement logic to inject necessary headers/cookies (for Crab Sports) and API keys (for Pinnacle) obtained from configuration.
* Log scrape execution status and detailed errors upon final failure (see NFRs).

### 5.2. Data Normalization

* **Game Identification:**
  * Implement an alias lookup table to map varying team names to a canonical `team_id`.
  * Use fuzzy matching (e.g., Levenshtein ratio ≥ 0.9) as a fallback for unresolved team names, logging misses for manual alias addition.
  * Generate a unique `game_id` based on a combination of `league`, canonical `home_team_id`, canonical `away_team_id`, and `scheduled_start_utc` (rounded to the nearest 5-minute bucket).
  * Allow a tolerance of ≤ 5 minutes for start times between books when matching games based on rounded UTC times.
* **Market Identification:**
  * Implement an alias lookup table (e.g., YAML/dict) to map raw market names (e.g., "Point Spread", "Handicap", "Run Line") to a canonical `market_type` enum (e.g., `SPREAD`).
  * Normalize line values (for Spreads, Totals):
    * Strip non-numeric characters.
    * Convert to `Decimal` type.
    * Quantize to the nearest 0.25 using `ROUND_HALF_EVEN`.
  * Generate a unique `market_id` based on `canonical_game_id`, `market_type`, `side` (e.g., home/away, over/under), normalized `line_value`, and `period` (e.g., full_game).

### 5.3. Calculation

* **Vig Removal:** Calculate vig-free probabilities (`p_novig`) from the sharp book's (Pinnacle) odds pairs for each market using a standard method (e.g., multiplicative, additive, Shin).
* **Expected Value (EV):** Calculate `EV% = (p_novig × target_decimal_odds) – 1` for markets present on both the sharp book and a target book.
* **Kelly Criterion:** Calculate `Kelly Fraction = (b * p - q) / b` where:
  * `b = target_decimal_odds - 1`
  * `p = p_novig` (vig-free sharp probability)
  * `q = 1 - p`
* Calculations should be triggered only when a market is present on both the sharp book and at least one target book.

### 5.4. Data Storage

* Store normalized and calculated data in a Supabase PostgreSQL database.
* Database schema should include tables for (at minimum): `games`, `markets`, `books`, `odds_comparisons`.
* The `odds_comparisons` table (or equivalent) should store fields like:
  * `canonical_game_id` (FK)
  * `market_id` (FK)
  * `target_book` (text/FK)
  * `sharp_book` (text/FK)
  * `target_odds_decimal` (numeric)
  * `sharp_odds_decimal` (numeric)
  * `sharp_prob_novig` (numeric)
  * `ev_percent` (numeric)
  * `kelly_fraction` (numeric)
  * `timestamp_collected` (timestamptz)
* Store all comparison results (positive, zero, negative EV). Raw JSON from APIs is not required for persistent storage in MVP.
* Handle partial success: If one book scrape fails, process and store data from successfully scraped books in that cycle.

### 5.5. Query Interface

* Provide a basic mechanism (e.g., Python script, CLI tool) to query the stored data.
* Allow filtering by: Sport, Market Type, EV% threshold (configurable, e.g., >= 1.0%).
* Allow sorting results by: EV%, Kelly Fraction.

## 6. Non-Functional Requirements

* **Performance:**
  * End-to-end cycle time (scrape-normalize-calculate-save) must complete in **≤ 45 seconds** on average.
  * Query interface responses for typical requests (e.g., top EV bets today) should return in **≤ 2 seconds**.
* **Reliability:**
  * Achieve a scrape cycle success rate of **≥ 95%** over 24 hours (success = all targeted books scraped without final failure).
  * Implement automated retries for failed scrape attempts per book:
    * Max **3 retries** (4 total attempts).
    * **Exponential backoff** delay (e.g., 1s, 4s, 9s).
    * Cancel retries if the next scheduled cycle is imminent.
  * Log detailed information upon final scrape failure (timestamp, book, URL/endpoint, error/status code, traceback snippet).
* **Security / Authentication:**
  * Store sensitive credentials (API keys, header/cookie values) securely using environment variables (`.env` file via `python-dotenv`).
  * Inject required headers/cookies for Crab Sports and API key for Pinnacle per request.
  * Mask sensitive credentials in logs.
  * Handle authentication failures (401/403) by logging a warning and skipping the book for that cycle (after retries). Manual refresh of Crab Sports cookies is acceptable for MVP.
* **Rate Limiting:**
  * Respect `Retry-After` headers or standard 429 "Too Many Requests" responses by pausing scraping for that specific book until the next cycle. Log a warning.
  * Implement basic user-agent randomization (from a small pool).
* **Maintainability:**
  * Use clear code structure, type hinting, and libraries like Pydantic for data validation/modeling.
  * Employ structured logging (e.g., Structlog, Loguru) for easier debugging.

## 7. Design Considerations / Mockups

* **Tech Stack:** Python, HTTPX (async requests), Pydantic (validation/models), Pandas (data manipulation), Numpy (calculations), Supabase-py (DB client), Tenacity (retries), Orjson (JSON parsing), python-dotenv (config), structured logging library (Structlog/Loguru).
* **Architecture:** Backend service, potentially deployable as a scheduled task (cron) or serverless function (cloud function).
* **Data Flow:** Scrape JSON → Normalize with Pydantic → Create/Manipulate Pandas DataFrame → Calculate EV/Kelly → Save structured data to Supabase → Query Supabase for results.

### 7.1 Development Environment & Tooling

* **Package Management:** `uv` is used for Python package installation and environment management.
* **Task Automation:** `Taskfile.yml` is used for running common development tasks (e.g., `task run`, `task test`, `task format`, `task lint`, `task typecheck`).
* **Core Dev Dependencies:** `pytest` (testing), `black` (formatting), `ruff` (linting), `mypy` (type checking), `rich` (rich terminal output), `loguru` (logging), `httpx` (HTTP client).
* **Project Scaffolding:** New projects are typically set up using a standard script (`uv-quickstart.sh`) that initializes `uv`, git, basic project structure, `Taskfile`, and installs core dependencies.
* **Editor Integration:** Development often utilizes the Cursor editor, with specific context and rules defined in `.cursor/rules/context.mdc` to guide AI assistance according to project standards (e.g., preferring `uv`, `Taskfile`, modern Python idioms).
* **Mockups:** Not applicable for this backend service MVP.

## 8. Success Metrics (MVP)

* **Scraping Reliability:** Achieve a cycle success rate of >= 95% over a 24-hour period.
* **Data Coverage:** Successfully scrape, normalize, and store odds for NBA, NHL, and MLB main markets (ML, Spread, Total) from Crab Sports, Pinnacle, and one other TBD book.
* **EV Opportunity Identification:** Identify and store at least 5 distinct positive EV opportunities (EV% > 0%) per day (averaged over 7 days).
* **Calculation Accuracy:** Kelly Criterion stake percentages are calculated and stored for 100% of identified positive EV opportunities (verified via spot checks).
* **Query Functionality:** Basic query interface allows retrieval and sorting of EV bets by specified criteria within the 2-second performance target.
* **Database Integrity:** Unified Supabase schema established and successfully populated by the pipeline.

## 9. Open Questions / Future Considerations

* **MVP Scope:** Which specific 3rd sportsbook will be included in the MVP?
* **Raw Data:** Revisit the need to store raw API JSON snapshots for debugging or historical analysis post-MVP.
* **Odds Change Tracking:** Implement a more sophisticated method for tracking odds changes over time (e.g., versioning, detailed timestamping per odds update).
* **Authentication Automation:** Develop automated processes for refreshing headers/cookies or tokens that expire frequently.
* **Scalability:**
  * Implement proxy rotation or more advanced anti-scraping measures if IP bans become an issue.
  * Optimize database queries and indexing for larger datasets.
  * Scale compute resources for scraping/processing if needed.
* **Feature Expansion:**
  * Add support for more sportsbooks.
  * Add support for more sports and market types (e.g., player props, futures).
  * Develop a web UI or more sophisticated API for interacting with the data.
  * Implement more advanced vig removal methods or allow selection.
  * Consider alternative sharp book sources or consensus lines.

---
