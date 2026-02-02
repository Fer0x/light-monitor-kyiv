import os
import json
import requests
from datetime import datetime
from typing import Optional

# Конфигурация
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
CONFIG_FILE = "config.json"
CACHE_FILE = "last_hash.txt"

# Названия дней недели
DAYS_UA = {
    0: "Понеділок",
    1: "Вівторок",
    2: "Середа",
    3: "Четвер",
    4: "П'ятниця",
    5: "Субота",
    6: "Неділя"
}


def load_config() -> dict:
    """Загружаем конфигурацию"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        # Значения по умолчанию
        return {
            "groups": ["GPV12.1", "GPV18.1"],
            "region": "kyiv"
        }


def get_data_url(region: str) -> str:
    """Формируем URL для данных"""
    return f"https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/{region}.json"


def format_hours(hours: float) -> str:
    """Склонение слова 'година'"""
    if hours == int(hours):
        hours = int(hours)
    
    if isinstance(hours, float):
        return f"{hours} години"
    
    if hours % 10 == 1 and hours % 100 != 11:
        return f"{hours} година"
    elif hours % 10 in [2, 3, 4] and hours % 100 not in [12, 13, 14]:
        return f"{hours} години"
    else:
        return f"{hours} годин"


def format_slot_time(slot: int) -> str:
    """Конвертирует номер слота (0-48) во время"""
    total_minutes = slot * 30
    hours = total_minutes // 60
    minutes = total_minutes % 60
    
    if hours == 24:
        return "24:00"
    
    return f"{hours:02d}:{minutes:02d}"


def fetch_data(region: str) -> Optional[dict]:
    """Получаем данные из репозитория"""
    try:
        url = get_data_url(region)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Ошибка получения данных: {e}")
        return None


def get_cached_hash() -> Optional[str]:
    """Получаем сохраненный хеш"""
    try:
        with open(CACHE_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def save_hash(hash_value: str):
    """Сохраняем хеш"""
    with open(CACHE_FILE, "w") as f:
        f.write(hash_value)


def build_schedule(day_data: dict) -> list[dict]:
    """
    Строим расписание с получасовыми интервалами.
    """
    slots = []
    
    for hour in range(1, 25):
        hour_key = str(hour)
        status = day_data.get(hour_key, "yes")
        
        if status == "yes":
            first_half = True
            second_half = True
        elif status == "no":
            first_half = False
            second_half = False
        elif status == "first":
            first_half = False
            second_half = True
        elif status == "second":
            first_half = True
            second_half = False
        elif status in ["maybe", "mfirst", "msecond"]:
            first_half = True
            second_half = True
        else:
            first_half = True
            second_half = True
        
        slots.append(first_half)
        slots.append(second_half)
    
    if not slots:
        return []
    
    periods = []
    current_status = slots[0]
    start_slot = 0
    
    for i in range(1, len(slots)):
        if slots[i] != current_status:
            hours = (i - start_slot) * 0.5
            periods.append({
                "start": format_slot_time(start_slot),
                "end": format_slot_time(i),
                "is_on": current_status,
                "hours": hours
            })
            current_status = slots[i]
            start_slot = i
    
    hours = (len(slots) - start_slot) * 0.5
    periods.append({
        "start": format_slot_time(start_slot),
        "end": format_slot_time(len(slots)),
        "is_on": current_status,
        "hours": hours
    })
    
    return periods


def format_schedule_message(schedule: list[dict], date: datetime) -> str:
    """Форматируем сообщение для одного дня"""
    day_name = DAYS_UA[date.weekday()]
    date_str = date.strftime("%d.%m")
    
    lines = [f"🗓 Графік відключень на {date_str} ({day_name}):"]
    lines.append("")
    
    total_on = 0.0
    total_off = 0.0
    
    for period in schedule:
        emoji = "🔋" if period["is_on"] else "🪫"
        hours_text = format_hours(period["hours"])
        
        if period["is_on"]:
            status_text = f"({hours_text} Світло є)"
        else:
            status_text = f"(Світла нема {hours_text})"
        
        lines.append(f"{emoji}{period['start']} - {period['end']} {status_text}")
        
        if period["is_on"]:
            total_on += period["hours"]
        else:
            total_off += period["hours"]
    
    lines.append("")
    lines.append(f"Світло є {format_hours(total_on)}")
    lines.append(f"Світла нема {format_hours(total_off)}")
    
    return "\n".join(lines)


def format_full_message(data: dict, groups: list[str]) -> Optional[str]:
    """Формируем полное сообщение для всех групп и дней"""
    fact_data = data.get("fact", {}).get("data", {})
    
    if not fact_data:
        return None
    
    sorted_days = sorted(fact_data.keys(), key=lambda x: int(x))
    
    all_group_messages = []
    
    for group in groups:
        # Проверяем, есть ли данные для этой группы
        has_data = any(group in fact_data.get(day_ts, {}) for day_ts in sorted_days)
        if not has_data:
            print(f"Група {group} не знайдена в даних, пропускаємо")
            continue
        
        group_num = group.replace("GPV", "")
        header = f"============ група {group_num} ============"
        
        day_messages = []
        for day_ts in sorted_days[:2]:
            day_data = fact_data[day_ts].get(group)
            if not day_data:
                continue
            
            date = datetime.fromtimestamp(int(day_ts))
            schedule = build_schedule(day_data)
            message = format_schedule_message(schedule, date)
            day_messages.append(message)
        
        if day_messages:
            days_text = "\n\n-------------------------------------\n".join(day_messages)
            all_group_messages.append(f"{header}\n{days_text}")
    
    return "\n\n\n".join(all_group_messages)


def send_telegram_message(message: str) -> bool:
    """Отправляем сообщение в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        print("Telegram credentials not configured")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    max_length = 4000
    
    if len(message) <= max_length:
        parts = [message]
    else:
        parts = message.split("\n\n\n")
    
    for part in parts:
        payload = {
            "chat_id": TELEGRAM_CHANNEL_ID,
            "text": part,
            "parse_mode": "HTML"
        }
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print("Повідомлення відправлено успішно")
        except Exception as e:
            print(f"Помилка відправки: {e}")
            return False
    
    return True


def main():
    # Загружаем конфигурацию
    config = load_config()
    groups = config.get("groups", ["GPV12.1", "GPV18.1"])
    region = config.get("region", "kyiv")
    
    print(f"Регіон: {region}")
    print(f"Групи: {', '.join(groups)}")
    print("Fetching data...")
    
    data = fetch_data(region)
    
    if not data:
        print("Failed to fetch data")
        return
    
    # Проверяем, есть ли обновления
    content_hash = data.get("meta", {}).get("contentHash", "")
    cached_hash = get_cached_hash()
    
    if content_hash == cached_hash:
        print("No updates detected")
        return
    
    print(f"New data detected! Hash: {content_hash[:16]}...")
    
    # Форматируем сообщение
    message = format_full_message(data, groups)
    
    if not message:
        print("Failed to format message")
        return
    
    print("Generated message:")
    print("-" * 50)
    print(message)
    print("-" * 50)
    
    # Отправляем в Telegram
    if send_telegram_message(message):
        save_hash(content_hash)
        print("Hash saved")
    else:
        print("Failed to send message, hash not saved")


if __name__ == "__main__":
    main()
