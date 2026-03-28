# Real-Time News Streaming System

A production-style realtime news pipeline with a clean Phase 2 processing layer:

```
Crawler -> Kafka(raw_news) -> Processor Service -> Kafka(processed_news) -> Telegram
```

Optional extras remain available:
- `Flink` profile for experimentation
- `storage` profile with `MinIO + Iceberg REST catalog + Iceberg writer + Trino`

Fully containerized with Docker. Runs on Apple Silicon (M1/M2/M3).

---

## 📁 Project Structure

```
demo_etl/
├── crawler-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py              # Async RSS fetcher + Kafka producer
├── telegram-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py              # Kafka consumer + Telegram sender
│   └── test_telegram.py     # Standalone bot test
├── processor-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── classifier.py
│   ├── normalizer.py
│   ├── main.py              # raw_news -> processed_news
│   └── test_processor.py
├── iceberg-writer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py              # processed_news -> Iceberg on MinIO
│   └── test_iceberg_writer.py
├── trino/
│   └── catalog/
│       └── iceberg.properties
├── flink-job/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── job.py               # Optional experimental profile
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 🤖 Step 1: Telegram Bot Setup (REQUIRED)

### 1.1 — Create a Bot
1. Open Telegram → search for **@BotFather**
2. Send `/newbot`
3. Choose a name (e.g. `My News Bot`) and username (e.g. `my_news_bot`)
4. Copy the **BOT_TOKEN** (format: `123456789:ABCdefGHI...`)

### 1.2 — Create a Channel
1. Telegram → New Channel → set name (e.g. **Real-Time News**)
2. Choose **Public** channel → set a username (e.g. `@my_news_channel`)
3. Create the channel

### 1.3 — Add Bot as Admin
1. Open your channel → Manage Channel → Administrators
2. Click **Add Administrator** → search for your bot username
3. Enable at least **Post Messages** permission → Save

### 1.4 — Get CHANNEL_ID
Use either format:
- **Public**: `@my_news_channel`
- **Numeric ID**: Use `@username_to_id_bot` in Telegram, or check `https://api.telegram.org/bot<TOKEN>/getUpdates`

---

## ⚙️ Step 2: Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
BOT_TOKEN=123456789:ABCdefGHIjklMNOPQRSTUVWXYZ   # From BotFather
CHANNEL_ID=@my_news_channel                         # Or -100xxxxxxxx
```

> ⚠️ **Never commit your `.env` file to version control.**

---

## ✅ Step 3: Test Telegram (Before Running Docker)

Verify your bot and channel work before starting the full stack:

```bash
cd telegram-service
pip install httpx python-dotenv
python test_telegram.py
```

Expected output:
```
[1/3] Verifying bot identity via getMe…
      ✓ Bot: @my_news_bot (id=123456789)
[2/3] Sending test message to @my_news_channel…
      ✓ Message sent! message_id=42
🎉 Everything looks good!
```

---

## 🐳 Step 4: Docker Desktop Configuration (Mac M-series)

### Recommended Settings
1. Docker Desktop → Settings → General:
   - Enable Rosetta for x86/amd64 emulation only if you plan to run the optional `flink` profile

2. Docker Desktop → Settings → Resources:
   - Memory: **≥4 GB** (6 GB recommended if you also run the optional `flink` profile)
   - CPUs: **≥2**

---

## 🟢 Phase 1: Run the MVP Pipeline

```bash
docker-compose up --build
```

This starts:
| Service | Role |
| --- | --- |
| `kafka` | Message broker |
| `kafka-init` | Creates topics (`raw_news`, `processed_news`) |
| `crawler-service` | Fetches RSS → publishes to `raw_news` |
| `telegram-service` | Consumes `raw_news` → sends to Telegram |

**Within ~30 seconds**, your Telegram channel will receive news articles.

### Check Logs

```bash
docker-compose logs -f crawler-service    # See crawled articles
docker-compose logs -f telegram-service   # See Telegram sends
```

### Verify Kafka

```bash
# List topics
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092

# Watch messages in raw_news
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic raw_news \
  --from-beginning \
  --max-messages 5
```

---

## 🟡 Phase 2: Processor Service

Phase 2 is the built-in `processor-service`. It consumes `raw_news`, performs deterministic enrichment, and publishes a cleaner `processed_news` stream.

Phase 2 responsibilities:
- deduplication within the current processor session
- rule-based category classification
- keyword extraction
- summary normalization
- stable `article_id`
- `published_at` semantics based on RSS publish time when available
- manual Kafka commit after successful downstream publish
- realtime crawl mode with startup-time filtering and persistent seen-URL state
- one-shot backfill mode with per-feed / total limits and optional `--days`

### 2.1 — Switch Telegram to `processed_news`

Edit `.env`:

```env
TELEGRAM_KAFKA_TOPIC=processed_news
```

### 2.2 — Start or rebuild the Phase 2 stack

```bash
docker compose up --build
```

If the stack is already running:

```bash
docker compose up -d --build processor-service telegram-service
```

### 2.3 — Realtime Mode vs Backfill Mode

The crawler has **two clearly-scoped modes**, controlled by the `CRAWLER_MODE` env var or `--mode` CLI flag.

#### Realtime Mode (default)

```
CRAWLER_MODE=realtime   # in .env
```

- Records `startup_ts` (Unix timestamp) when the service starts.
- **Only publishes articles where `published_ts >= startup_ts`.**
- Articles with **no `published_ts`** in the RSS feed are **skipped** (no timestamp guarantee).
- Uses a persistent state file to prevent re-sending the same URL across subsequent poll cycles.
- First boot does **not** flush any backlog already in the feeds.

This is the safe default: you will only receive articles that were published *after* you started the system.

#### Backfill Mode (historical ingest)

```bash
# Publish everything currently in all feeds (no limit)
docker compose run --rm crawler-service python main.py --mode backfill

# Last 7 days only, at most 100 articles per feed
docker compose run --rm crawler-service python main.py --mode backfill --days 7 --per-feed-limit 100

# Hard total cap across all feeds
docker compose run --rm crawler-service python main.py --mode backfill --total-limit 500

# All three combined
docker compose run --rm crawler-service \
  python main.py --mode backfill --days 14 --per-feed-limit 200 --total-limit 2000
```

**Backfill flags:**

| Flag | Description |
|---|---|
| `--days N` | Only include articles published in the last N days. Uses `published_ts`. Articles with no timestamp are **included** (conservative). |
| `--per-feed-limit N` | Stop each individual feed after N articles are published. |
| `--total-limit N` | Hard cap across all feeds combined. |

Backfill **does not** use the persistent state file — it intentionally re-publishes all found articles.

#### Keyword Rules and Alerting

Keywords in `feeds.yaml` (`include_keywords`, `exclude_keywords`) control the `should_alert` field that is set on each article. They do **not** block ingest — every article is still published to Kafka. The `should_alert` field is used downstream by the Telegram service to decide whether to send the article to the channel.

### 2.4 — Telegram Offset Reset Behaviour

When Telegram starts with a **new consumer group** (first run or after `docker compose down -v`):

| `KAFKA_AUTO_OFFSET_RESET` | Behaviour |
|---|---|
| `latest` (default) | Starts from the most recent message — no backlog replay. Correct for realtime mode. |
| `earliest` | Replays all messages from the beginning — use this with backfill so the articles reach Telegram. |

Set in `.env`:
```env
KAFKA_AUTO_OFFSET_RESET=earliest  # alongside a backfill run
KAFKA_AUTO_OFFSET_RESET=latest    # normal realtime operation (default)
```

> **Note:** This setting only affects **new** consumer groups. If the group already has committed offsets (e.g. you ran previously), Kafka ignores `auto_offset_reset` and picks up where it left off.
>
> To make switching between modes easier, the Telegram service scopes the effective group ID by crawler mode by default:
> `telegram-consumer-group-realtime` and `telegram-consumer-group-backfill`.
> This behaviour is controlled by `KAFKA_GROUP_ID_SCOPE_BY_MODE=true`.

### 2.5 — Verify Phase 2

```bash
docker compose logs -f processor-service
docker compose logs -f telegram-service

# Watch processed_news topic
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic processed_news \
  --from-beginning \
  --max-messages 3
```
You should see `processor-service` logging successful publishes into `processed_news`, then `telegram-service` consuming those enriched records.

## 🧪 Optional: Experimental Flink Profile

The old Flink-based processing path is still available as an optional profile for experimentation:

```bash
docker compose --profile flink up --build
```

Use it only if you explicitly want to experiment with Flink on top of the main stack.

---

## 🟣 Phase 3: Storage Profile (`MinIO + Iceberg + Trino`)

Phase 3 now has a real storage path. When you enable the `storage` profile:

```text
Kafka(processed_news)
  -> iceberg-writer
  -> Iceberg table news.processed_news
  -> MinIO bucket news-archive
  -> Trino SQL queries
```

What is real today:
- `iceberg-writer` consumes `processed_news`
- it creates or loads the Iceberg table `news.processed_news`
- data files and metadata are stored under `s3://news-archive/warehouse/...` in MinIO
- Trino is preconfigured with an Iceberg REST catalog and can query that table

What is still not built yet:
- a separate Bronze raw table for `raw_news`
- downstream Gold aggregate tables
- retention/compaction jobs

### 3.1 — Start the storage stack

```bash
docker compose --profile storage up --build
```

This adds:
| Service | Role |
| --- | --- |
| `minio` | S3-compatible object storage |
| `iceberg-rest` | Iceberg metadata catalog |
| `iceberg-writer` | Consumes `processed_news` and appends to Iceberg |
| `trino` | SQL query engine over the Iceberg catalog |

### 3.2 — Verify writer activity

```bash
docker compose logs -f iceberg-writer
```

You should see logs confirming:
- the MinIO bucket exists or was created
- the Iceberg table `news.processed_news` is ready
- batches are appended and Kafka offsets are committed after the append

### 3.3 — Query via Trino

Show schemas:

```bash
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
```

Show tables:

```bash
docker exec trino trino --execute "SHOW TABLES FROM iceberg.news"
```

Read recent rows:

```bash
docker exec trino trino --execute "
SELECT article_id, source, title, published_at, should_alert
FROM iceberg.news.processed_news
ORDER BY kafka_offset DESC
LIMIT 10
"
```

### 3.4 — Where MinIO data is stored

Inside this project, MinIO data is not written as normal files in the repo tree.
It lives in the Docker volume `minio-data`, mounted in the container at `/data`.

The Iceberg catalog metadata store is persisted in the Docker volume `iceberg-rest-data`.

---

## 🛑 Stopping the System

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (resets Kafka data)
docker-compose down -v
```

---

## 🔧 Troubleshooting

### ❌ Telegram: 403 Forbidden

**Symptom:** `Telegram returned 403 Forbidden.`

**Cause:** Bot is NOT an admin of the channel.

**Fix:**
1. Open your channel settings → Administrators
2. Add your bot as administrator with **Post Messages** permission
3. Run `python test_telegram.py` again to confirm

---

### ❌ Telegram: Wrong CHANNEL_ID

**Symptom:** `400 Bad Request: chat not found`

**Fix:** Try both formats:
- `@channelname` (public channels)
- `-100xxxxxxxxx` (use `@username_to_id_bot` to get the ID)

---

### ❌ Kafka: Services Can't Connect

**Symptom:** `NoBrokersAvailable` or `Connection refused`

**Fix:**
```bash
# Kafka is only accessible as kafka:9092 inside Docker
# From host, use localhost:9093
docker-compose logs kafka | tail -20
docker-compose restart kafka
```

Wait for Kafka healthcheck to pass:
```bash
docker inspect kafka | grep -A 5 '"Health"'
```

---

### ❌ Optional Flink: Platform / Architecture Issues

**Symptom:** `exec format error` or image pulling fails

**Fix:** 
1. In Docker Desktop → Settings → General → enable **"Use Rosetta for x86/amd64 emulation"**
2. Restart Docker Desktop
3. Pull fresh:
   ```bash
   docker-compose --profile flink pull
   docker-compose --profile flink up --build
   ```

---

### ❌ No News Appearing in Telegram

**Checklist:**
1. `docker-compose logs telegram-service` — look for `✓ Sent to Telegram`
2. `docker-compose logs crawler-service` — look for `Published:` lines
3. Confirm `.env` has correct `BOT_TOKEN` and `CHANNEL_ID`
4. Re-run `python telegram-service/test_telegram.py`

---

### ❌ Adjust Crawl Frequency

Default: **60 seconds**. To change:

```env
CRAWL_INTERVAL_SECONDS=30   # in .env
```

---

## 📊 Data Schemas

### `raw_news` (Kafka Topic)
```json
{
  "id": "uuid",
  "title": "Tiêu đề bài viết",
  "content": "Mô tả ngắn...",
  "url": "https://...",
  "source": "VNExpress",
  "timestamp": 1711234567,
  "published_ts": 1711230000
}
```

### `processed_news` (Kafka Topic — Phase 2)
```json
{
  "article_id": "6f9b9e6cc2d8b7d7f8e49d4fe1adf1aa",
  "title": "Tiêu đề bài viết",
  "summary": "Tóm tắt...",
  "url": "https://...",
  "keywords": ["ai", "openai", "cloud"],
  "category": "Công nghệ",
  "source": "VNExpress",
  "published_at": "2026-03-28T03:12:00Z",
  "ingested_at": "2026-03-28T03:12:18Z",
  "raw_hash": "2adf...",
  "is_duplicate": false
}
```

---

## 🌐 RSS Sources

| Source | URL |
|---|---|
| VNExpress - Tin mới nhất | `https://vnexpress.net/rss/tin-moi-nhat.rss` |
| VNExpress - Thời sự | `https://vnexpress.net/rss/thoi-su.rss` |
| VNExpress - Thế giới | `https://vnexpress.net/rss/the-gioi.rss` |
| VNExpress - Kinh doanh | `https://vnexpress.net/rss/kinh-doanh.rss` |
| BBC World News | `https://feeds.bbci.co.uk/news/world/rss.xml` |

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────┐
│              Docker Network (news-net)            │
│                                                    │
│  ┌──────────────┐      raw_news      ┌──────────┐  │
│  │ crawler      │ ─────────────────► │  kafka   │  │
│  │ service      │                    │ :9092    │  │
│  └──────────────┘                    └────┬─────┘  │
│                                           │         │
│                                 ┌─────────▼────────┐│
│                                 │ processor-service ││
│                                 │ classify+dedup    ││
│                                 └─────────┬────────┘│
│                                           │          │
│                                    processed_news   │
│                                           │          │
│                                 ┌─────────▼────────┐│
│                                 │ telegram-service ││
│                                 └─────────┬────────┘│
└───────────────────────────────────────────┼──────────┘
                                            │
                                      ┌─────▼─────┐
                                      │ Telegram  │
                                      │ Channel   │
                                      └───────────┘
```
