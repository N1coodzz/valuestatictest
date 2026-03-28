import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("8697436760:AAFO7FjyAZ30DuVR2u1OLwQ11Bj43b_eY3g")
OWNER_ID = int(os.getenv("OWNER_ID"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

DB_PATH = "data/bot.db"
