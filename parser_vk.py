import time
import re
import pandas as pd
import vk_api
from auth_vk import VK_TOKEN
from collections import defaultdict

# ✅ Ключевые слова — любые из них делают пост релевантным (о зданиях/наследии)
keywords = [
    r"памятник(а|и|ов|у|ом|е)?\s*(архитектур(ы|е|ой|у)?)?",
    r"объект(а|ы|ов)?\s+культурн(ого|ой)?\s+(наследия|ценности)?",
    r"историческ(ое|ий|ая|ого|ому|им|их|ие)?\s+здан(ие|ия|ий|ию|ием|и)?",
    r"дом(а|ов)?\s+(стар(ый|ого|ые)|дореволюционн(ый|ого|ые))",
    r"здание.*(памятник|историческ|архитектурн)",
    r"архитектурн(ый|ая|ое|ые|ого)?\s+объект(ы|ов)?",
    r"ценн(ый|ое|ая|ые|ых)?\s+градоформирующ(ий|ие)?\s+объект(ы|ов)?"
]

# 🔎 Фильтр на географию
def matches_criteria(text):
    if not text:
        return False
    text_lower = text.lower()
    if "курск" in text_lower or "курская область" in text_lower:
        return False
    return any(re.search(pattern, text_lower) for pattern in keywords)

# 🧾 Имена пользователей
custom_names = {
    "id14099614": "Анастасия Кнор",
    "ondryushka": "Андрей Кочетков",
    "public168139760": "ГИООКН Самарской области",
}

# 📡 VK API
vk_session = vk_api.VkApi(token=VK_TOKEN)
vk = vk_session.get_api()

# 📥 Список аккаунтов
usernames = [
    'tomsawyerfest',
    'dgsamara',
    'irafishman',
    'voopiik_samara',
    'samaralastdream',
    'public168139760',
    'a_hinshtein',
    'fedorischev63',
    'ondryushka',
    'id14099614',
]

# 📊 Столбцы
columns = [
    "Канал", "ID", "Дата", "Текст", "Просмотры", "Ответы", "Форварды", "Лайки"
]

# 🔍 Получение ID
def get_user_id(username):
    try:
        res = vk.utils.resolveScreenName(screen_name=username)
        if res and "object_id" in res:
            return res["object_id"], res["type"]
    except Exception as e:
        print(f"❌ Ошибка ID @{username}: {e}")
    return None, None

# 📥 Сбор постов
def get_filtered_posts(username, max_posts=1000):
    uid, obj_type = get_user_id(username)
    if not uid:
        return []

    owner_id = -uid if obj_type == "group" else uid
    offset = 0
    batch_size = 100
    posts = []

    while offset < max_posts:
        try:
            response = vk.wall.get(owner_id=owner_id, count=batch_size, offset=offset)
            items = response.get("items", [])
            if not items:
                break

            for post in items:
                text = post.get("text", "")
                if matches_criteria(text):
                    display_name = custom_names.get(username, username)
                    likes_count = post.get("likes", {}).get("count", 0)

                    post_data = {
                        "Канал": display_name,
                        "ID": post.get("id"),
                        "Дата": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(post["date"])),
                        "Текст": text.replace("\n", " ").strip(),
                        "Просмотры": post.get("views", {}).get("count", 0),
                        "Ответы": post.get("comments", {}).get("count", 0),
                        "Форварды": post.get("reposts", {}).get("count", 0),
                        "Лайки": likes_count
                    }
                    posts.append(post_data)

            offset += batch_size
            time.sleep(0.35)  # ограничение VK API

        except Exception as e:
            print(f"❌ Ошибка при получении постов от @{username}: {e}")
            break

    print(f"📦 Найдено {len(posts)} релевантных постов в @{username}")
    return posts

# 🚀 Запуск
all_data = []
for username in usernames:
    print(f"🔍 Обработка @{username}")
    posts = get_filtered_posts(username)
    all_data.extend(posts)

# 💾 Сохраняем
df = pd.DataFrame(all_data, columns=columns)
df.to_csv("vk_filtered_posts_broad.csv", index=False, encoding='utf-8-sig')
print("✅ Сохранено в файл 'vk_filtered_posts_broad.csv'")
