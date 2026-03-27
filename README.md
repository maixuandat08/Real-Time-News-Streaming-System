# 📡 Real-Time News Streaming System

A production-style real-time news pipeline:

```
Crawler → Kafka → (Flink Enrichment) → Telegram Service → Telegram Channel
```

Fully containerized with Docker. Runs on **Apple Silicon (M1/M2/M3)**.

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
├── flink-job/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── job.py               # Stream enrichment + deduplication
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
   - ✅ **Use Rosetta for x86/amd64 emulation** (required for Flink)

2. Docker Desktop → Settings → Resources:
   - Memory: **≥4 GB** (6 GB recommended for Phase 2 with Flink)
   - CPUs: **≥2**

---

## 🟢 Phase 1: Run the MVP Pipeline

```bash
docker-compose up --build
```

This starts:
| Service | Role |
|---|---|
| `zookeeper` | Kafka coordination |
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

## 🟡 Phase 2: Add Stream Processing (Flink)

The Flink job enriches articles with:
- **Deduplication** (10-min TTL window)
- **Keyword extraction** (Vietnamese + English taxonomy)
- **Category classification** (Chính trị, Kinh tế, Công nghệ, etc.)
- **Summary generation**

### 2.1 — Start Flink

```bash
docker-compose --profile flink up --build
```

This adds:
| Service | Role |
|---|---|
| `flink-jobmanager` | Flink coordinator (Web UI: `http://localhost:8081`) |
| `flink-taskmanager` | Flink worker |
| `flink-job` | Submits the enrichment job |

### 2.2 — Switch Telegram to `processed_news`

Edit `.env`:

```env
TELEGRAM_KAFKA_TOPIC=processed_news
```

Restart telegram-service:

```bash
docker-compose restart telegram-service
```

### 2.3 — Verify Flink

```bash
docker-compose logs -f flink-job         # See enrichment logs
docker-compose logs -f flink-jobmanager  # Check job status

# Watch processed_news topic
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic processed_news \
  --from-beginning \
  --max-messages 3
```

Open Flink Web UI: **http://localhost:8081**

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

### ❌ Flink: Platform / Architecture Issues

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
  "timestamp": 1711234567
}
```

### `processed_news` (Kafka Topic — Phase 2)
```json
{
  "id": "uuid",
  "title": "Tiêu đề bài viết",
  "summary": "Tóm tắt...",
  "url": "https://...",
  "keywords": ["kinh tế", "đầu tư"],
  "category": "Kinh tế",
  "source": "VNExpress",
  "timestamp": 1711234567,
  "processed_at": 1711234580
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
┌─────────────────────────────────────────────────┐
│                  Docker Network (news-net)        │
│                                                   │
│  ┌──────────────┐    raw_news     ┌───────────┐   │
│  │   crawler    │ ─────────────► │   kafka   │   │
│  │  (Python)    │                │  :9092    │   │
│  └──────────────┘                └─────┬─────┘   │
│                                        │          │
│                             ┌──────────┴──────┐  │
│                             │                 │  │
│                   ┌─────────▼──────┐  ┌───────▼──┐│
│                   │  flink-job     │  │ telegram  ││
│                   │ (Phase 2)      │  │ -service  ││
│                   │ dedup+enrich   │  │           ││
│                   └───────┬────────┘  └──────┬────┘│
│                           │ processed_news   │     │
│                           └──────────────────┘     │
└─────────────────────────────────────────────────────┘
                                           │
                                     ┌─────▼──────┐
                                     │  Telegram  │
                                     │  Channel   │
                                     └────────────┘
```
