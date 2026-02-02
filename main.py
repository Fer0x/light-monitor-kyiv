import os
import json
import hashlib
import requests
from datetime import datetime
from typing import Optional

# Конфигурация
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
CONFIG_FILE = "config.json"
CACHE_FILE = "last_hash.txt"

# URLs
GITHUB_DATA_URL = "https://raw.githubusercontent.com/Baskerville42/outage-data-ua/main/data/{region}.json"
YASNO_API_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/{region_id}/dsos/{dso_id}/planned-outages"

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

# Названия источников
SOURCE_GITHUB = "outage-data-ua"
SOURCE_YASNO = "app.yasno.ua"


def load_config() -> dict:
    """Загружаем конфигурацию"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "groups": ["GPV12.1", "GPV18.1"],
            "region": "kyiv",
            "yasno_region_id": "25",
            "yasno_dso_id": "902"
        }


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


def format_time(minutes: int) -> str:
    """Конвертирует минуты в строку времени"""
    hours = minutes // 60
    mins = minutes % 60
    
    if hours == 24:
        return "24:00"
    
    return f"{hours:02d}:{mins:02d}"


def format_slot_time(slot: int) -> str:
    """Конвертирует номер слота (0-48) во время"""
    return format_time(slot * 30)


# ==================== GITHUB DATA SOURCE ====================

def fetch_github_data(region: str) -> Optional[dict]:
    """Получаем данные из GitHub репозитория"""
    try:
        url = GITHUB_DATA_URL.format(region=region)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Помилка отримання даних з GitHub: {e}")
        return None


def parse_github_day(day_data: dict) -> list[bool]:
    """
    Парсим данные дня из GitHub в массив 48 получасовых слотов.
    True = свет есть, False = света нет
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
    
    return slots


def extract_github_schedules(data: dict, groups: list[str]) -> dict:
    """
    Извлекаем расписания из GitHub данных.
    Возвращает: {group: {date_str: [48 slots]}}
    """
    result = {}
    fact_data = data.get("fact", {}).get("data", {})
    
    if not fact_data:
        return result
    
    sorted_days = sorted(fact_data.keys(), key=lambda x: int(x))
    
    for group in groups:
        result[group] = {}
        
        for day_ts in sorted_days[:2]:
            day_data = fact_data.get(day_ts, {}).get(group)
            if not day_data:
                continue
            
            date = datetime.fromtimestamp(int(day_ts))
            date_str = date.strftime("%Y-%m-%d")
            
            slots = parse_github_day(day_data)
            result[group][date_str] = {
                "slots": slots,
                "date": date
            }
    
    return result


# ==================== YASNO API SOURCE ====================

def fetch_yasno_data(region_id: str, dso_id: str) -> Optional[dict]:
    """Получаем данные из Yasno API"""
    try:
        url = YASNO_API_URL.format(region_id=region_id, dso_id=dso_id)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Помилка отримання даних з Yasno API: {e}")
        return None


def parse_yasno_day(day_data: dict) -> list[bool]:
    """
    Парсим данные дня из Yasno API в массив 48 получасовых слотов.
    True = свет есть (NotPlanned), False = света нет (Definite)
    """
    slots = [True] * 48  # По умолчанию свет есть
    
    if not day_data or "slots" not in day_data:
        return slots
    
    for slot in day_data["slots"]:
        start_min = slot.get("start", 0)
        end_min = slot.get("end", 0)
        slot_type = slot.get("type", "NotPlanned")
        
        # Конвертируем минуты в индексы получасовых слотов
        start_idx = start_min // 30
        end_idx = end_min // 30
        
        is_on = (slot_type == "NotPlanned")
        
        for i in range(start_idx, min(end_idx, 48)):
            slots[i] = is_on
    
    return slots


def extract_yasno_schedules(data: dict, groups: list[str]) -> dict:
    """
    Извлекаем расписания из Yasno API данных.
    Возвращает: {group: {date_str: [48 slots]}}
    """
    result = {}
    
    if not data:
        return result
    
    for group in groups:
        # Убираем GPV префикс для поиска в Yasno данных
        group_key = group.replace("GPV", "")
        
        if group_key not in data:
            continue
        
        group_data = data[group_key]
        result[group] = {}
        
        for day_key in ["today", "tomorrow"]:
            day_data = group_data.get(day_key)
            if not day_data or "date" not in day_data:
                continue
            
            # Парсим дату
            date_str_full = day_data["date"]
            date = datetime.fromisoformat(date_str_full.replace("+02:00", "+00:00").replace("+00:00", ""))
            date_str = date.strftime("%Y-%m-%d")
            
            slots = parse_yasno_day(day_data)
            result[group][date_str] = {
                "slots": slots,
                "date": date
            }
    
    return result


# ==================== SCHEDULE COMPARISON ====================

def slots_to_periods(slots: list[bool]) -> list[dict]:
    """Конвертируем массив слотов в список периодов"""
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
    
    # Последний период
    hours = (len(slots) - start_slot) * 0.5
    periods.append({
        "start": format_slot_time(start_slot),
        "end": format_slot_time(len(slots)),
        "is_on": current_status,
        "hours": hours
    })
    
    return periods


def schedules_match(slots1: list[bool], slots2: list[bool]) -> bool:
    """Проверяем, совпадают ли два расписания"""
    if len(slots1) != len(slots2):
        return False
    return slots1 == slots2


# ==================== MESSAGE FORMATTING ====================

def format_schedule_message(periods: list[dict], date: datetime, sources: list[str]) -> str:
    """Форматируем сообщение для одного дня"""
    day_name = DAYS_UA[date.weekday()]
    date_str = date.strftime("%d.%m")
    sources_str = ", ".join(sources)
    
    lines = [f"🗓 Графік відключень на {date_str} ({day_name}) [{sources_str}]:"]
    lines.append("")
    
    total_on = 0.0
    total_off = 0.0
    
    for period in periods:
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


def format_group_message(
    group: str,
    github_schedules: dict,
    yasno_schedules: dict
) -> Optional[str]:
    """Форматируем сообщение для одной группы"""
    
    group_num = group.replace("GPV", "")
    header = f"============ група {group_num} ============"
    
    # Собираем все даты
    all_dates = set()
    if group in github_schedules:
        all_dates.update(github_schedules[group].keys())
    if group in yasno_schedules:
        all_dates.update(yasno_schedules[group].keys())
    
    if not all_dates:
        return None
    
    sorted_dates = sorted(all_dates)[:2]  # Только два дня
    day_messages = []
    
    for date_str in sorted_dates:
        github_data = github_schedules.get(group, {}).get(date_str)
        yasno_data = yasno_schedules.get(group, {}).get(date_str)
        
        github_slots = github_data["slots"] if github_data else None
        yasno_slots = yasno_data["slots"] if yasno_data else None
        date = github_data["date"] if github_data else yasno_data["date"]
        
        if github_slots and yasno_slots:
            # Оба источника есть - сравниваем
            if schedules_match(github_slots, yasno_slots):
                # Данные совпадают - один блок с обоими источниками
                periods = slots_to_periods(github_slots)
                msg = format_schedule_message(periods, date, [SOURCE_GITHUB, SOURCE_YASNO])
                day_messages.append(msg)
            else:
                # Данные НЕ совпадают - два отдельных блока
                github_periods = slots_to_periods(github_slots)
                yasno_periods = slots_to_periods(yasno_slots)
                
                msg1 = format_schedule_message(github_periods, date, [SOURCE_GITHUB])
                msg2 = format_schedule_message(yasno_periods, date, [SOURCE_YASNO])
                
                day_messages.append(msg1)
                day_messages.append(msg2)
        
        elif github_slots:
            # Только GitHub
            periods = slots_to_periods(github_slots)
            msg = format_schedule_message(periods, date, [SOURCE_GITHUB])
            day_messages.append(msg)
        
        elif yasno_slots:
            # Только Yasno
            periods = slots_to_periods(yasno_slots)
            msg = format_schedule_message(periods, date, [SOURCE_YASNO])
            day_messages.append(msg)
    
    if not day_messages:
        return None
    
    days_text = "\n\n-------------------------------------\n".join(day_messages)
    return f"{header}\n{days_text}"


def format_full_message(
    github_schedules: dict,
    yasno_schedules: dict,
    groups: list[str]
) -> Optional[str]:
    """Формируем полное сообщение"""
    
    all_group_messages = []
    
    for group in groups:
        msg = format_group_message(group, github_schedules, yasno_schedules)
        if msg:
            all_group_messages.append(msg)
    
    if not all_group_messages:
        return None
    
    return "\n\n\n".join(all_group_messages)


# ==================== CACHING ====================

def compute_combined_hash(github_data: Optional[dict], yasno_data: Optional[dict]) -> str:
    """Вычисляем хеш от комбинированных данных"""
    combined = {
        "github": github_data.get("meta", {}).get("contentHash", "") if github_data else "",
        "yasno": json.dumps(yasno_data, sort_keys=True) if yasno_data else ""
    }
    return hashlib.sha256(json.dumps(combined, sort_keys=True).encode()).hexdigest()


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


# ==================== TELEGRAM ====================

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
            "text": part
        }
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print("Повідомлення відправлено успішно")
        except Exception as e:
            print(f"Помилка відправки: {e}")
            return False
    
    return True


# ==================== MAIN ====================

def main():
    # Загружаем конфигурацию
    config = load_config()
    groups = config.get("groups", ["GPV12.1", "GPV18.1"])
    region = config.get("region", "kyiv")
    yasno_region_id = config.get("yasno_region_id", "25")
    yasno_dso_id = config.get("yasno_dso_id", "902")
    
    print(f"Регіон: {region}")
    print(f"Групи: {', '.join(groups)}")
    print(f"Yasno: region={yasno_region_id}, dso={yasno_dso_id}")
    
    # Получаем данные из обоих источников
    print("\nFetching GitHub data...")
    github_data = fetch_github_data(region)
    
    print("Fetching Yasno API data...")
    yasno_data = fetch_yasno_data(yasno_region_id, yasno_dso_id)
    
    if not github_data and not yasno_data:
        print("Failed to fetch data from both sources")
        return
    
    # Проверяем обновления
    combined_hash = compute_combined_hash(github_data, yasno_data)
    cached_hash = get_cached_hash()
    
    if combined_hash == cached_hash:
        print("No updates detected")
        return
    
    print(f"New data detected! Hash: {combined_hash[:16]}...")
    
    # Извлекаем расписания
    github_schedules = extract_github_schedules(github_data, groups) if github_data else {}
    yasno_schedules = extract_yasno_schedules(yasno_data, groups) if yasno_data else {}
    
    print(f"\nGitHub groups: {list(github_schedules.keys())}")
    print(f"Yasno groups: {list(yasno_schedules.keys())}")
    
    # Форматируем сообщение
    message = format_full_message(github_schedules, yasno_schedules, groups)
    
    if not message:
        print("Failed to format message - no data available")
        return
    
    print("\nGenerated message:")
    print("-" * 50)
    print(message)
    print("-" * 50)
    
    # Отправляем в Telegram
    if send_telegram_message(message):
        save_hash(combined_hash)
        print("Hash saved")
    else:
        print("Failed to send message, hash not saved")


if __name__ == "__main__":
    main()
