import csv
import asyncio
import re
from collections import defaultdict
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import my_config  # должен содержать api_id, api_hash и tnumber

# Конфигурация
api_id = my_config.api_id
api_hash = my_config.api_hash
tnumber = my_config.tnumber

channels = [
    "drugoigorod", "truekpru63", "chp_samara", "samartop", "samara_smi",
    "news63ru", "samara_vip", "samara", "samaray", "samara5", "matveevkomment", "enter_samara",
    "Hinshtein", "Fedorischev63", "save_as_samara"
]

output_file = "tg_filtered_posts_broad.csv"

# 📌 Ключевые слова — только упоминания объектов
keywords = [
    r"памятник(и|а|ов)?( архитектур(ы|е|ой)?)?",
    r"объект(ы)? культурн(ого наследия|ой ценности)?",
    r"историческ(ое|ий|ая|ого|ому|им|их|ие)? здани(е|я|ий|ем)?",
    r"дом(а|ов)? (стар(ый|ого|ые)|дореволюционн(ый|ого|ые))",
    r"архитектурн(ый|ая|ое|ые)? объект(ы)?",
    r"ценн(ое|ый|ая|ые)? градоформирующ(ий|ие)? объект(ы)?"
]

# ❌ Исключение по географии (Курская область)
def matches_criteria(text):
    if not text:
        return False
    text_lower = text.lower()
    if "курск" in text_lower or "курская область" in text_lower:
        return False
    return any(re.search(pattern, text_lower) for pattern in keywords)

async def fetch_posts():
    async with TelegramClient(tnumber, api_id, api_hash) as client:
        all_data = []
        all_emojis = set()

        for channel_username in channels:
            try:
                print(f"🔍 Обработка канала @{channel_username}")
                entity = await client.get_entity(channel_username)
                offset_id = 0
                limit = 100

                while True:
                    history = await client(GetHistoryRequest(
                        peer=entity,
                        offset_id=offset_id,
                        offset_date=None,
                        add_offset=0,
                        limit=limit,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))

                    messages = history.messages
                    if not messages:
                        break

                    for msg in messages:
                        text = msg.message
                        if matches_criteria(text):
                            reaction_counts = defaultdict(int)
                            if msg.reactions and msg.reactions.results:
                                for reaction in msg.reactions.results:
                                    emoji = getattr(reaction.reaction, "emoticon", "🔒")
                                    reaction_counts[emoji] += reaction.count
                                    all_emojis.add(emoji)

                            data = {
                                "Канал": channel_username,
                                "ID": msg.id,
                                "Дата": msg.date.strftime("%Y-%m-%d %H:%M"),
                                "Текст": text.replace("\n", " ").strip() if text else "",
                                "Просмотры": msg.views or 0,
                                "Ответы": msg.replies.replies if msg.replies else 0,
                                "Форварды": msg.forwards or 0,
                                "Реакции": reaction_counts
                            }
                            all_data.append(data)

                    offset_id = messages[-1].id

                print(f"✅ Завершена загрузка из @{channel_username}")

            except Exception as e:
                print(f"❌ Ошибка при обработке @{channel_username}: {e}")

        # Сохраняем в CSV
        emoji_columns = sorted(all_emojis)
        fieldnames = ["Канал", "ID", "Дата", "Текст", "Просмотры", "Ответы", "Форварды"] + emoji_columns

        with open(output_file, mode="w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for item in all_data:
                row = {
                    "Канал": item["Канал"],
                    "ID": item["ID"],
                    "Дата": item["Дата"],
                    "Текст": item["Текст"],
                    "Просмотры": item["Просмотры"],
                    "Ответы": item["Ответы"],
                    "Форварды": item["Форварды"],
                }
                for emoji in emoji_columns:
                    row[emoji] = item["Реакции"].get(emoji, 0)
                writer.writerow(row)

        print(f"\n📁 Данные сохранены в файл: {output_file}")

if __name__ == "__main__":
    asyncio.run(fetch_posts())
