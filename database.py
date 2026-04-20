# -*- coding: utf-8 -*-
"""
Модуль для работы с базой данных
Оптимизирован для высокой нагрузки
"""

import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
import json


class Database:
    """Потокобезопасная работа с базой данных"""
    
    def __init__(self, db_path: str, pool_size: int = 20):
        self.db_path = db_path
        self.pool_size = pool_size
        self.local = threading.local()
        self._init_db()
    
    def _get_connection(self):
        """Получить соединение для текущего потока"""
        if not hasattr(self.local, 'connection'):
            self.local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self.local.connection.row_factory = sqlite3.Row
            # Оптимизация для производительности
            self.local.connection.execute("PRAGMA journal_mode=WAL")
            self.local.connection.execute("PRAGMA synchronous=NORMAL")
            self.local.connection.execute("PRAGMA cache_size=10000")
            self.local.connection.execute("PRAGMA temp_store=MEMORY")
        return self.local.connection

    def _column_exists(self, cursor, table_name: str, column_name: str) -> bool:
        """Проверить существование колонки в таблице"""
        cursor.execute(f"PRAGMA table_info({table_name})")
        return any(row[1] == column_name for row in cursor.fetchall())

    def _ensure_column(self, cursor, table_name: str, column_name: str, definition: str):
        """Добавить колонку, если ее еще нет"""
        if not self._column_exists(cursor, table_name, column_name):
            cursor.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
            )
    
    @contextmanager
    def get_cursor(self):
        """Контекстный менеджер для работы с курсором"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def _init_db(self):
        """Инициализация структуры базы данных"""
        with self.get_cursor() as cursor:
            # Таблица пользователей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_request_date INTEGER,
                    approved_date INTEGER,
                    is_approved INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)

            self._ensure_column(cursor, "users", "broadcast_chat_id", "INTEGER")
            self._ensure_column(cursor, "users", "broadcast_chat_source", "TEXT")
            self._ensure_column(cursor, "users", "broadcast_chat_expires_at", "INTEGER")
            
            # Таблица для отслеживания rate limiting
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    user_id INTEGER,
                    action TEXT,
                    timestamp INTEGER,
                    PRIMARY KEY (user_id, action, timestamp)
                )
            """)
            
            # Таблица статистики
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT,
                    user_id INTEGER,
                    data TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)
            
            # Индексы для оптимизации
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_approved 
                ON users(is_approved)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_rate_limits_user_time 
                ON rate_limits(user_id, timestamp)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_statistics_type_time 
                ON statistics(event_type, timestamp)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_statistics_user_type_time
                ON statistics(user_id, event_type, timestamp)
            """)
    
    def add_user(
        self,
        user_id: int,
        username: str = None,
        first_name: str = None,
        last_name: str = None,
        join_request_date: Optional[int] = None,
        broadcast_chat_id: Optional[int] = None,
        broadcast_chat_source: Optional[str] = None,
        broadcast_chat_expires_at: Optional[int] = None
    ) -> bool:
        """Добавить или обновить пользователя"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, join_request_date)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        join_request_date = COALESCE(excluded.join_request_date, users.join_request_date)
                """, (user_id, username, first_name, last_name, join_request_date))

                if broadcast_chat_id is not None:
                    if broadcast_chat_source == "start":
                        cursor.execute("""
                            UPDATE users
                            SET broadcast_chat_id = ?,
                                broadcast_chat_source = ?,
                                broadcast_chat_expires_at = NULL
                            WHERE user_id = ?
                        """, (broadcast_chat_id, broadcast_chat_source, user_id))
                    else:
                        cursor.execute("""
                            UPDATE users
                            SET broadcast_chat_id = ?,
                                broadcast_chat_source = ?,
                                broadcast_chat_expires_at = ?
                            WHERE user_id = ?
                              AND COALESCE(broadcast_chat_source, '') != 'start'
                        """, (
                            broadcast_chat_id,
                            broadcast_chat_source,
                            broadcast_chat_expires_at,
                            user_id
                        ))
            return True
        except Exception as e:
            print(f"Error adding user: {e}")
            return False
    
    def approve_user(self, user_id: int) -> bool:
        """Отметить пользователя как одобренного"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE users 
                    SET is_approved = 1, approved_date = ?
                    WHERE user_id = ?
                """, (int(time.time()), user_id))
            return True
        except Exception as e:
            print(f"Error approving user: {e}")
            return False
    
    def is_user_approved(self, user_id: int) -> bool:
        """Проверить, одобрен ли пользователь"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT is_approved FROM users WHERE user_id = ?
                """, (user_id,))
                result = cursor.fetchone()
                return result and result[0] == 1
        except Exception:
            return False
    
    def check_rate_limit(self, user_id: int, action: str, 
                        max_requests: int, period: int) -> bool:
        """
        Проверить rate limit для пользователя
        Возвращает True если лимит не превышен
        """
        try:
            current_time = int(time.time())
            cutoff_time = current_time - period
            
            with self.get_cursor() as cursor:
                # Удалить старые записи
                cursor.execute("""
                    DELETE FROM rate_limits 
                    WHERE user_id = ? AND action = ? AND timestamp < ?
                """, (user_id, action, cutoff_time))
                
                # Подсчитать текущие запросы
                cursor.execute("""
                    SELECT COUNT(*) FROM rate_limits 
                    WHERE user_id = ? AND action = ? AND timestamp >= ?
                """, (user_id, action, cutoff_time))
                
                count = cursor.fetchone()[0]
                
                if count >= max_requests:
                    return False
                
                # Добавить новую запись
                cursor.execute("""
                    INSERT INTO rate_limits (user_id, action, timestamp)
                    VALUES (?, ?, ?)
                """, (user_id, action, current_time))
                
                return True
        except Exception as e:
            print(f"Error checking rate limit: {e}")
            return True  # В случае ошибки разрешаем запрос
    
    def log_event(self, event_type: str, user_id: int = None, data: Dict = None):
        """Записать событие в статистику"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO statistics (event_type, user_id, data)
                    VALUES (?, ?, ?)
                """, (event_type, user_id, json.dumps(data) if data else None))
        except Exception as e:
            print(f"Error logging event: {e}")

    def has_recent_join_request(
        self,
        user_id: int,
        chat_id: int,
        max_age_seconds: Optional[int] = None
    ) -> bool:
        """Проверить, есть ли у пользователя недавняя заявка в канал"""
        try:
            query = """
                SELECT data
                FROM statistics
                WHERE user_id = ? AND event_type = 'join_request'
            """
            params = [user_id]

            if max_age_seconds is not None:
                query += " AND timestamp >= ?"
                params.append(int(time.time()) - max_age_seconds)

            query += " ORDER BY timestamp DESC"

            with self.get_cursor() as cursor:
                cursor.execute(query, params)

                for row in cursor.fetchall():
                    raw_data = row["data"] if isinstance(row, sqlite3.Row) else row[0]
                    if not raw_data:
                        continue

                    try:
                        payload = json.loads(raw_data)
                    except (TypeError, json.JSONDecodeError):
                        continue

                    if payload.get("chat_id") == chat_id:
                        return True

            return False
        except Exception as e:
            print(f"Error checking recent join request: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику бота"""
        try:
            with self.get_cursor() as cursor:
                # Общее количество пользователей
                cursor.execute("SELECT COUNT(*) FROM users")
                total_users = cursor.fetchone()[0]
                
                # Одобренные пользователи
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_approved = 1")
                approved_users = cursor.fetchone()[0]
                
                # Ожидающие одобрения
                pending_users = total_users - approved_users
                
                # Статистика за последние 24 часа
                day_ago = int(time.time()) - 86400
                cursor.execute("""
                    SELECT COUNT(*) FROM users WHERE join_request_date >= ?
                """, (day_ago,))
                requests_24h = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE approved_date >= ? AND is_approved = 1
                """, (day_ago,))
                approved_24h = cursor.fetchone()[0]
                
                return {
                    "total_users": total_users,
                    "approved_users": approved_users,
                    "pending_users": pending_users,
                    "requests_24h": requests_24h,
                    "approved_24h": approved_24h
                }
        except Exception as e:
            print(f"Error getting statistics: {e}")
            return {}
    
    def get_all_users(self) -> List[int]:
        """Получить список всех user_id для рассылки"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT user_id FROM users")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting all users: {e}")
            return []

    def get_broadcast_targets(self) -> List[Dict[str, Any]]:
        """Получить доступные чаты для рассылки"""
        try:
            current_time = int(time.time())

            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT user_id
                    FROM statistics
                    WHERE event_type = 'start_command'
                """)
                started_users = {row[0] for row in cursor.fetchall()}

                cursor.execute("""
                    SELECT user_id, broadcast_chat_id, broadcast_chat_source, broadcast_chat_expires_at
                    FROM users
                """)

                targets = []
                for row in cursor.fetchall():
                    user_id = row["user_id"]
                    broadcast_chat_id = row["broadcast_chat_id"]
                    broadcast_chat_source = row["broadcast_chat_source"]
                    broadcast_chat_expires_at = row["broadcast_chat_expires_at"]

                    chat_id = None
                    source = None

                    if broadcast_chat_source == "start":
                        chat_id = broadcast_chat_id or user_id
                        source = "start"
                    elif broadcast_chat_source == "join_request":
                        if broadcast_chat_id and broadcast_chat_expires_at and broadcast_chat_expires_at >= current_time:
                            chat_id = broadcast_chat_id
                            source = "join_request"
                    elif user_id in started_users:
                        chat_id = user_id
                        source = "legacy_start"

                    if chat_id is not None:
                        targets.append({
                            "user_id": user_id,
                            "chat_id": chat_id,
                            "source": source
                        })

                return targets
        except Exception as e:
            print(f"Error getting broadcast targets: {e}")
            return []
    
    def cleanup_old_data(self, days: int = 30):
        """Очистка старых данных для оптимизации"""
        try:
            cutoff_time = int(time.time()) - (days * 86400)
            with self.get_cursor() as cursor:
                # Удалить старые rate limits
                cursor.execute("""
                    DELETE FROM rate_limits WHERE timestamp < ?
                """, (cutoff_time,))
                
                # Удалить старую статистику
                cursor.execute("""
                    DELETE FROM statistics WHERE timestamp < ?
                """, (cutoff_time,))
        except Exception as e:
            print(f"Error cleaning up old data: {e}")
