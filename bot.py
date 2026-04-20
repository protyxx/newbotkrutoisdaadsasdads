# -*- coding: utf-8 -*-
"""
Основной модуль Telegram бота
Обработка заявок в канал с проверкой подписок
ОПТИМИЗИРОВАННАЯ ВЕРСИЯ для высоких нагрузок
"""

import asyncio
import time
from typing import Dict, Set, Optional, Tuple
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from telegram.error import TelegramError, BadRequest, Forbidden

from config import (
    BOT_TOKEN,
    ADMIN_ID,
    TARGET_CHANNEL_ID,
    TARGET_CHANNEL_NAME,
    REQUIRED_CHANNELS,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_PERIOD,
    CHECK_SUBSCRIPTION_COOLDOWN,
    WORKERS_COUNT,
    API_TIMEOUT,
    HTTP_POOL_SIZE,
    MAX_CONNECTIONS,
    SUBSCRIPTION_CACHE_TTL,
    JOIN_REQUEST_CONTACT_TTL,
    PENDING_JOIN_REQUEST_TTL,
    MESSAGES
)
from database import Database


class SubscriptionCache:
    """Кэш для результатов проверки подписок"""
    
    def __init__(self, ttl: int = 30):
        self.cache: Dict[Tuple[int, int], Tuple[bool, float]] = {}
        self.ttl = ttl
        self.lock = asyncio.Lock()
    
    async def get(self, user_id: int, channel_id: int) -> Optional[bool]:
        """Получить результат из кэша"""
        async with self.lock:
            key = (user_id, channel_id)
            if key in self.cache:
                result, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return result
                else:
                    del self.cache[key]
            return None
    
    async def set(self, user_id: int, channel_id: int, is_subscribed: bool):
        """Сохранить результат в кэш"""
        async with self.lock:
            self.cache[(user_id, channel_id)] = (is_subscribed, time.time())
    
    async def invalidate(self, user_id: int):
        """Инвалидировать кэш для пользователя"""
        async with self.lock:
            keys_to_delete = [k for k in self.cache.keys() if k[0] == user_id]
            for key in keys_to_delete:
                del self.cache[key]
    
    async def cleanup(self):
        """Очистка устаревших записей"""
        async with self.lock:
            current_time = time.time()
            keys_to_delete = [
                k for k, (_, timestamp) in self.cache.items()
                if current_time - timestamp >= self.ttl
            ]
            for key in keys_to_delete:
                del self.cache[key]


class TelegramBot:
    """Основной класс бота с оптимизацией"""
    
    def __init__(self):
        self.db = Database("bot_database.db")
        self.user_cooldowns: Dict[int, float] = {}
        self.processing_users: Set[int] = set()
        self.subscription_cache = SubscriptionCache(ttl=SUBSCRIPTION_CACHE_TTL)
        self.required_channel_ids = {
            channel["id"] for channel in REQUIRED_CHANNELS
            if isinstance(channel["id"], int)
        }
        self.tracked_join_request_channels = self.required_channel_ids | {TARGET_CHANNEL_ID}
        
        # Семафор для ограничения одновременных запросов к API
        self.api_semaphore = asyncio.Semaphore(100)
        
        # Флаг для фоновой задачи
        self.cleanup_task_started = False
    
    async def _cache_cleanup_task(self):
        """Фоновая задача для очистки кэша"""
        while True:
            await asyncio.sleep(60)
            await self.subscription_cache.cleanup()
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        # Запустить фоновую задачу очистки кэша при первом вызове
        if not self.cleanup_task_started:
            self.cleanup_task_started = True
            asyncio.create_task(self._cache_cleanup_task())
        
        user = update.effective_user
        
        # Добавить пользователя в базу асинхронно
        asyncio.create_task(asyncio.to_thread(
            self.db.add_user,
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            broadcast_chat_id=user.id,
            broadcast_chat_source="start"
        ))
        
        asyncio.create_task(asyncio.to_thread(
            self.db.log_event, "start_command", user.id
        ))
        
        await update.message.reply_text(
            MESSAGES["welcome"],
            parse_mode="HTML"
        )
        
        # Отправить сообщение с требованием подписки после /start
        await self.send_subscription_message(user.id, context)
    
    async def handle_join_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик заявки на вступление в канал"""
        user = update.chat_join_request.from_user
        chat = update.chat_join_request.chat
        user_chat_id = update.chat_join_request.user_chat_id
        join_request_date = int(time.time())

        if chat.id not in self.tracked_join_request_channels:
            return

        asyncio.create_task(asyncio.to_thread(
            self.db.log_event, "join_request", user.id, {"chat_id": chat.id}
        ))

        # Заявки в обязательные каналы просто учитываем для проверки подписки.
        # Допуск в основной канал выполняется только по заявке в TARGET_CHANNEL_ID.
        if chat.id != TARGET_CHANNEL_ID:
            return
        
        # Проверка rate limit асинхронно
        is_allowed = await asyncio.to_thread(
            self.db.check_rate_limit,
            user.id,
            "join_request",
            RATE_LIMIT_REQUESTS,
            RATE_LIMIT_PERIOD
        )
        
        if not is_allowed:
            try:
                await context.bot.send_message(
                    chat_id=user_chat_id,
                    text=MESSAGES["rate_limit"]
                )
            except:
                pass
            return
        
        # Добавить пользователя в базу асинхронно
        asyncio.create_task(asyncio.to_thread(
            self.db.add_user,
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            join_request_date=join_request_date,
            broadcast_chat_id=user_chat_id,
            broadcast_chat_source="join_request",
            broadcast_chat_expires_at=join_request_date + JOIN_REQUEST_CONTACT_TTL
        ))
        
        # Отправить сообщение с требованием подписки сразу
        # НЕ одобряем заявку автоматически - только после проверки подписок
        await self.send_subscription_message(user_chat_id, context)

    async def has_pending_join_request(self, user_id: int, channel_id) -> bool:
        """Проверить, есть ли у пользователя недавняя заявка в приватный канал"""
        if not isinstance(channel_id, int):
            return False

        return await asyncio.to_thread(
            self.db.has_recent_join_request,
            user_id,
            channel_id,
            PENDING_JOIN_REQUEST_TTL
        )
    
    async def send_subscription_message(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Отправить сообщение с требованием подписки на каналы"""
        channels_list = "\n".join([
            f"• <a href='{channel['url']}'>{channel['name']}</a>" for channel in REQUIRED_CHANNELS
        ])
        
        message_text = MESSAGES["welcome"].format(
            channel_name=TARGET_CHANNEL_NAME,
            channels_list=channels_list
        )
        
        keyboard = [[
            InlineKeyboardButton("✅ Проверить подписки", callback_data="check_subscriptions")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Error sending subscription message to {chat_id}: {e}")
    
    async def check_subscriptions_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатия кнопки проверки подписок"""
        query = update.callback_query
        user = query.from_user
        
        await query.answer()
        
        # Проверка cooldown
        current_time = time.time()
        if user.id in self.user_cooldowns:
            time_left = CHECK_SUBSCRIPTION_COOLDOWN - (current_time - self.user_cooldowns[user.id])
            if time_left > 0:
                await query.message.reply_text(
                    MESSAGES["cooldown"].format(seconds=int(time_left))
                )
                return
        
        self.user_cooldowns[user.id] = current_time
        
        # Проверка rate limit
        is_allowed = await asyncio.to_thread(
            self.db.check_rate_limit,
            user.id,
            "check_subscription",
            RATE_LIMIT_REQUESTS,
            RATE_LIMIT_PERIOD
        )
        
        if not is_allowed:
            await query.message.reply_text(MESSAGES["rate_limit"])
            return
        
        # Предотвращение одновременной обработки
        if user.id in self.processing_users:
            return
        
        self.processing_users.add(user.id)
        
        try:
            # Редактировать существующее сообщение вместо отправки нового
            await query.edit_message_text(MESSAGES["checking_subscriptions"])
            
            # Инвалидировать кэш перед проверкой
            await self.subscription_cache.invalidate(user.id)
            
            # Проверить подписки на все каналы параллельно
            tasks = [
                self.check_user_subscription(user.id, channel["id"], context)
                for channel in REQUIRED_CHANNELS
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            not_subscribed = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Error checking subscription: {result}")
                    continue
                
                if not result:
                    not_subscribed.append({
                        "name": REQUIRED_CHANNELS[i]["name"],
                        "url": REQUIRED_CHANNELS[i]["url"]
                    })
            
            if not_subscribed:
                # Пользователь не подписан на все каналы
                channels_list = "\n".join([
                    f"• <a href='{ch['url']}'>{ch['name']}</a>" for ch in not_subscribed
                ])
                
                # Создать новую клавиатуру с кнопкой проверки
                keyboard = [[
                    InlineKeyboardButton("✅ Проверить подписки", callback_data="check_subscriptions")
                ]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    MESSAGES["not_subscribed"].format(channels_list=channels_list),
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
                asyncio.create_task(asyncio.to_thread(
                    self.db.log_event, "subscription_check_failed", user.id,
                    {"not_subscribed": [ch["name"] for ch in not_subscribed]}
                ))
            else:
                # Все подписки есть - одобрить заявку
                try:
                    await context.bot.approve_chat_join_request(
                        chat_id=TARGET_CHANNEL_ID,
                        user_id=user.id
                    )
                    
                    asyncio.create_task(asyncio.to_thread(
                        self.db.approve_user, user.id
                    ))
                    asyncio.create_task(asyncio.to_thread(
                        self.db.log_event, "user_approved", user.id
                    ))
                    
                    await query.edit_message_text(
                        MESSAGES["success"],
                        parse_mode="HTML"
                    )
                except BadRequest as e:
                    if "user is already a participant" in str(e).lower():
                        asyncio.create_task(asyncio.to_thread(
                            self.db.approve_user, user.id
                        ))
                        await query.edit_message_text(MESSAGES["already_member"])
                    else:
                        await query.edit_message_text(MESSAGES["error"])
                        print(f"Error approving user {user.id}: {e}")
                except Exception as e:
                    await query.edit_message_text(MESSAGES["error"])
                    print(f"Error approving user {user.id}: {e}")
        
        finally:
            self.processing_users.discard(user.id)
    
    async def check_user_subscription(self, user_id: int, channel_id, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Проверить подписку пользователя на канал с кэшированием"""
        
        # Проверить кэш
        cached_result = await self.subscription_cache.get(user_id, channel_id)
        if cached_result is not None:
            return cached_result
        
        try:
            async with self.api_semaphore:
                member = await context.bot.get_chat_member(
                    chat_id=channel_id,
                    user_id=user_id
                )
                
                # Проверить статус участника
                # Только реальные подписчики:
                # - MEMBER - обычный участник канала
                # - ADMINISTRATOR - администратор
                # - OWNER - владелец
                # - RESTRICTED - участник с ограничениями
                is_subscribed = member.status in [
                    ChatMemberStatus.MEMBER,
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.OWNER,
                    ChatMemberStatus.RESTRICTED
                ]
                
                # Для приватных каналов с заявками проверяем дополнительно
                # Если статус LEFT, проверяем есть ли недавно поданная заявка
                if not is_subscribed and member.status == ChatMemberStatus.LEFT:
                    is_subscribed = await self.has_pending_join_request(user_id, channel_id)
                
                # Сохранить в кэш
                await self.subscription_cache.set(user_id, channel_id, is_subscribed)
                
                return is_subscribed
        except (BadRequest, Forbidden) as e:
            error_msg = str(e).lower()
            # В приватных каналах Telegram иногда не возвращает статус участника
            # для пользователя с активной заявкой, поэтому перепроверяем по логам заявок.
            if (
                "user not found" in error_msg
                or "participant_id_invalid" in error_msg
                or "chat not found" in error_msg
            ):
                is_subscribed = await self.has_pending_join_request(user_id, channel_id)
                await self.subscription_cache.set(user_id, channel_id, is_subscribed)
                return is_subscribed
            print(f"Error checking subscription for user {user_id} in channel {channel_id}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error checking subscription: {e}")
            return False
    
    async def admin_stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для просмотра статистики (только для админа)"""
        user = update.effective_user
        
        if user.id != ADMIN_ID:
            return
        
        stats = await asyncio.to_thread(self.db.get_statistics)
        
        stats_text = f"""
📊 <b>Статистика бота</b>

👥 Всего пользователей: {stats.get('total_users', 0)}
✅ Одобрено: {stats.get('approved_users', 0)}
⏳ Ожидают: {stats.get('pending_users', 0)}

📈 За последние 24 часа:
• Заявок: {stats.get('requests_24h', 0)}
• Одобрено: {stats.get('approved_24h', 0)}
"""
        
        await update.message.reply_text(stats_text, parse_mode="HTML")
    
    async def admin_broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для начала рассылки (только для админа)"""
        user = update.effective_user
        
        if user.id != ADMIN_ID:
            return
        
        await update.message.reply_text(
            "📢 Отправьте сообщение для рассылки (текст или фото с подписью).\n"
            "Используйте /cancel для отмены."
        )
        
        context.user_data['broadcast_mode'] = True
    
    async def handle_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка сообщения для рассылки с оптимизацией"""
        user = update.effective_user
        
        if user.id != ADMIN_ID or not context.user_data.get('broadcast_mode'):
            return
        
        context.user_data['broadcast_mode'] = False
        
        users = await asyncio.to_thread(self.db.get_all_users)
        targets = await asyncio.to_thread(self.db.get_broadcast_targets)
        unreachable_count = max(len(users) - len(targets), 0)
        
        await update.message.reply_text(
            f"🚀 Начинаю рассылку.\n"
            f"Всего в базе: {len(users)}\n"
            f"Доступно для отправки сейчас: {len(targets)}\n"
            f"Недоступно без прямого чата с ботом: {unreachable_count}"
        )
        
        success_count = 0
        fail_count = 0
        
        # Разбить на батчи для параллельной обработки
        batch_size = 30  # Telegram limit: 30 messages per second
        
        async def send_to_user(target: Dict[str, int]):
            nonlocal success_count, fail_count
            try:
                chat_id = target["chat_id"]
                if update.message.photo:
                    await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=update.message.photo[-1].file_id,
                        caption=update.message.caption,
                        parse_mode="HTML"
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=update.message.text,
                        parse_mode="HTML"
                    )
                success_count += 1
            except Exception as e:
                fail_count += 1
        
        # Отправка батчами
        for i in range(0, len(targets), batch_size):
            batch = targets[i:i + batch_size]
            tasks = [send_to_user(target) for target in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Задержка между батчами (1 секунда для 30 сообщений)
            if i + batch_size < len(targets):
                await asyncio.sleep(1)
        
        await update.message.reply_text(
            f"✅ Рассылка завершена!\n"
            f"Успешно: {success_count}\n"
            f"Ошибок: {fail_count}\n"
            f"Пропущено: {unreachable_count}"
        )
        
        asyncio.create_task(asyncio.to_thread(
            self.db.log_event, "broadcast", ADMIN_ID,
            {"success": success_count, "failed": fail_count}
        ))
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена текущей операции"""
        user = update.effective_user
        
        if user.id == ADMIN_ID:
            context.user_data['broadcast_mode'] = False
            await update.message.reply_text("❌ Операция отменена.")
    
    def run(self):
        """Запуск бота с оптимизированными настройками"""
        # Создать приложение с максимальной производительностью
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .connection_pool_size(HTTP_POOL_SIZE)
            .pool_timeout(API_TIMEOUT)
            .read_timeout(API_TIMEOUT)
            .write_timeout(API_TIMEOUT)
            .connect_timeout(API_TIMEOUT)
            .get_updates_connection_pool_size(MAX_CONNECTIONS)
            .get_updates_pool_timeout(API_TIMEOUT)
            .get_updates_read_timeout(API_TIMEOUT)
            .get_updates_write_timeout(API_TIMEOUT)
            .get_updates_connect_timeout(API_TIMEOUT)
            .build()
        )
        
        # Регистрация обработчиков
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("stats", self.admin_stats_command))
        application.add_handler(CommandHandler("broadcast", self.admin_broadcast_command))
        application.add_handler(CommandHandler("cancel", self.cancel_command))
        application.add_handler(ChatJoinRequestHandler(self.handle_join_request))
        application.add_handler(CallbackQueryHandler(
            self.check_subscriptions_callback,
            pattern="^check_subscriptions$"
        ))
        application.add_handler(MessageHandler(
            filters.TEXT | filters.PHOTO,
            self.handle_broadcast_message
        ))
        
        print("Bot zapuschen i gotov k rabote!")
        print(f"Workerov: {WORKERS_COUNT}")
        print(f"Rate limit: {RATE_LIMIT_REQUESTS} zaprosov/{RATE_LIMIT_PERIOD}s")
        print(f"HTTP Pool: {HTTP_POOL_SIZE}")
        print(f"Max Connections: {MAX_CONNECTIONS}")
        print(f"Cache TTL: {SUBSCRIPTION_CACHE_TTL}s")
        
        # Запуск бота
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )


if __name__ == "__main__":
    bot = TelegramBot()
    bot.run()
