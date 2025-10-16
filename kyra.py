import logging
import re
import csv
import os
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# === 🔑 НАСТРОЙКИ ===
BOT_TOKEN = '7622730743:AAGhoElXAziGCWGNdURlUoNTdpaDVedHp2M'
GROUP_CHAT_ID = -1002942758131   # ← ОБЯЗАТЕЛЬНО замени на правильный ID!
TIMEZONE = 'Europe/Moscow'
CSV_FILE = 'deliveries.csv'

# === 📝 Логирование ===
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === 🗃️ Инициализация CSV (создаём файл с заголовками, если не существует) ===
def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'courier_user_id', 'courier_username', 'amount', 'message_id', 'status'])
        logger.info(f"Создан новый файл: {CSV_FILE}")

# === 📥 Парсинг сообщения ===
def parse_delivery_message(text: str):
    match = re.search(r'\+(\d+)\s+@?(\w+)', text)
    if match:
        try:
            amount = int(match.group(1))
            username = match.group(2)
            return username, amount
        except ValueError:
            pass
    return None, None

# === 📬 Обработчик сообщений ===
def handle_group_message(update: Update, context: CallbackContext):
    if update.effective_chat.id != GROUP_CHAT_ID:
        return

    user_id = update.effective_user.id
    username = update.effective_user.username or 'unknown'
    message_id = update.message.message_id
    text = (update.message.text or "").strip()

    parsed_user, amount = parse_delivery_message(text)
    if not parsed_user or not amount:
        logger.debug(f"Не распознано: {text}")
        return

    # Получаем текущее время в нужной зоне
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    # Добавляем запись в CSV
    try:
        with open(CSV_FILE, mode='a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([now, user_id, username, amount, message_id, 'delivered'])
        logger.info(f"✅ Заказ сохранён: @{username} — {amount} руб.")
    except Exception as e:
        logger.error(f"Ошибка записи в CSV: {e}")

# === 📊 Считаем статистику за неделю из CSV ===
def get_weekly_stats():
    tz = pytz.timezone(TIMEZONE)
    today = datetime.now(tz).date()
    start_of_week = today - timedelta(days=today.weekday())

    stats = {}
    if not os.path.exists(CSV_FILE):
        return []

    try:
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Парсим дату
                    ts = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S').date()
                    if ts < start_of_week:
                        continue
                    if row.get('status') != 'delivered':
                        continue

                    user_id = row['courier_user_id']
                    username = row['courier_username'] or 'unknown'
                    amount = int(row['amount'])

                    key = (user_id, username)
                    if key not in stats:
                        stats[key] = 0
                    stats[key] += amount
                except Exception as e:
                    logger.warning(f"Пропущена некорректная строка: {row} | Ошибка: {e}")
    except Exception as e:
        logger.error(f"Ошибка чтения CSV: {e}")
        return []

    # Сортируем по убыванию суммы
    sorted_stats = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    return [(username, user_id, total) for ((user_id, username), total) in sorted_stats]
def stats_command(update: Update, context: CallbackContext):
    stats = get_weekly_stats()
    if not stats:
        update.message.reply_text("За эту неделю нет завершённых заказов.")
        return

    message = "📊 *Статистика за неделю:*\n\n"
    
    # Первый в списке — лучший (сортировка по убыванию)
    best_username, best_user_id, best_total = stats[0]
    best_mention = f"@{best_username}" if best_username != 'unknown' else f"user_{best_user_id}"
    
    # Добавляем заголовок с лучшим
    message += f"🥇 *Лучший курьер недели: {best_mention} — {best_total} руб.!*\n\n"
    
    # Остальные курьеры
    for username, user_id, total in stats:
        mention = f"@{username}" if username != 'unknown' else f"user_{user_id}"
        if username == best_username and user_id == best_user_id:
            continue  # уже показали
        message += f"{mention}: *{total} руб.*\n"
    
    if len(stats) == 1:
        message += "_Ты сражаешься в одиночку... но держишь план! 💪_"

    update.message.reply_text(message, parse_mode='Markdown')

# === 📅 Еженедельный отчёт ===
def send_weekly_report(context: CallbackContext):
    stats = get_weekly_stats()
    if not stats:
        message = "На этой неделе не было заказов 😢"
    else:
        best_username, best_user_id, best_total = stats[0]
        best_mention = f"@{best_username}" if best_username != 'unknown' else f"user_{best_user_id}"
        
        message = "📆 *Еженедельный отчёт по курьерам*\n\n"
        message += f"🏆 *Лучший курьер недели:*\n{best_mention} — *{best_total} руб.!*\n\n"
        message += "📋 Остальные:\n"
        
        for username, user_id, total in stats:
            mention = f"@{username}" if username != 'unknown' else f"user_{user_id}"
            if username == best_username and user_id == best_user_id:
                continue
            message += f"• {mention}: *{total} руб.*\n"
        
        if len(stats) == 1:
            message += "\n_Герой-одиночка этой недели! Респект!_"

    context.bot.send_message(chat_id=GROUP_CHAT_ID, text=message, parse_mode='Markdown')

# === ▶️ Запуск ===
def main():
    init_csv()
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(MessageHandler(Filters.chat(GROUP_CHAT_ID) & Filters.text, handle_group_message))
    dp.add_handler(CommandHandler("stats", stats_command))

    tz = pytz.timezone(TIMEZONE)
    scheduler = BackgroundScheduler(timezone=tz)
    scheduler.start()
    scheduler.add_job(
        send_weekly_report,
        CronTrigger(day_of_week=0, hour=9, minute=0, timezone=tz),
        args=[updater.job_queue]
    )

    updater.start_polling()
    logger.info("✅ Бот запущен! Данные сохраняются в deliveries.csv")
    updater.idle()

if __name__ == '__main__':
    main()