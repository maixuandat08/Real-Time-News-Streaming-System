# conftest.py — telegram-service
# test_format_message.py already uses importlib to load main.py by absolute path,
# so no sys.path manipulation is needed.
# This file exists to ensure pytest treats telegram-service as a proper test root.
