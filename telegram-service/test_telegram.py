"""
Standalone Telegram Bot Test Script

Run this BEFORE starting Docker to verify your bot/channel setup.

Usage:
    cp .env.example .env
    # Fill in BOT_TOKEN and CHANNEL_ID in .env
    pip install httpx python-dotenv
    python test_telegram.py
"""

import os
import sys
import json
import time

# Load .env from the telegram-service directory or parent
try:
    from dotenv import load_dotenv
    # Try parent directory first (project root), then current dir
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
    load_dotenv()  # fallback: current directory
except ImportError:
    pass  # dotenv is optional for this script

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install httpx")
    sys.exit(1)


def main():
    print("=" * 50)
    print("Telegram Bot Connection Test")
    print("=" * 50)

    bot_token = os.environ.get("BOT_TOKEN", "").strip()
    channel_id = os.environ.get("CHANNEL_ID", "").strip()

    if not bot_token:
        print("\n❌ FATAL: BOT_TOKEN is not set.")
        print("   Set it in your .env file or as an environment variable.")
        print("   Example: export BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrSTUvwxYZ")
        sys.exit(1)

    if not channel_id:
        print("\n❌ FATAL: CHANNEL_ID is not set.")
        print("   Set it in your .env file or as an environment variable.")
        print("   Example: export CHANNEL_ID=@yourchannel")
        print("        OR: export CHANNEL_ID=-1001234567890")
        sys.exit(1)

    print(f"\n✓ BOT_TOKEN: {bot_token[:10]}...{bot_token[-5:]} (masked)")
    print(f"✓ CHANNEL_ID: {channel_id}")

    api_url = f"https://api.telegram.org/bot{bot_token}"

    # Step 1: Verify bot identity
    print("\n[1/3] Verifying bot identity via getMe…")
    with httpx.Client() as client:
        try:
            resp = client.get(f"{api_url}/getMe", timeout=10)
            data = resp.json()
            if data.get("ok"):
                bot = data["result"]
                print(f"      ✓ Bot: @{bot['username']} (id={bot['id']})")
            else:
                print(f"      ❌ getMe failed: {data.get('description')}")
                print("         Check your BOT_TOKEN is correct.")
                sys.exit(1)
        except httpx.RequestError as e:
            print(f"      ❌ Network error: {e}")
            sys.exit(1)

        # Step 2: Send test message
        print(f"\n[2/3] Sending test message to {channel_id}…")
        test_message = (
            "🔔 <b>News Bot Test Message</b>\n\n"
            "✅ Your Telegram bot is configured correctly!\n"
            "The real-time news pipeline is ready to broadcast.\n\n"
            f"<i>Sent at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )

        payload = {
            "chat_id": channel_id,
            "text": test_message,
            "parse_mode": "HTML",
        }

        try:
            resp = client.post(f"{api_url}/sendMessage", json=payload, timeout=10)
            data = resp.json()
            if data.get("ok"):
                msg_id = data["result"]["message_id"]
                print(f"      ✓ Message sent! message_id={msg_id}")
                print(f"        Check your channel — you should see the test message.")
            elif resp.status_code == 403:
                print("      ❌ 403 Forbidden.")
                print("         → Make sure the bot is added as an ADMIN to the channel.")
                print(f"        → Channel: {channel_id}")
                sys.exit(1)
            elif resp.status_code == 400:
                print(f"      ❌ 400 Bad Request: {data.get('description')}")
                print("         → Check CHANNEL_ID format: @channelusername or -100xxxxxxxxx")
                sys.exit(1)
            else:
                print(f"      ❌ Unexpected response: {resp.status_code} — {data}")
                sys.exit(1)
        except httpx.RequestError as e:
            print(f"      ❌ Network error: {e}")
            sys.exit(1)

        # Step 3: Summary
        print("\n[3/3] Summary")
        print("      ✓ Bot token valid")
        print("      ✓ Channel reachable")
        print("      ✓ Test message sent successfully")
        print("\n🎉 Everything looks good! You can now run:")
        print("   docker-compose up --build")
        print("=" * 50)


if __name__ == "__main__":
    main()
