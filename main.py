#!/usr/bin/env python3
"""
S3 Sync Script - высокопроизводительное копирование между S3 хранилищами
Использует asyncio + aioboto3 для максимальной скорости
Оптимизирован для миллионов файлов
"""

import asyncio
import os
import sys
import mimetypes
import signal
from typing import Dict, List, Tuple, Optional

import aioboto3
import urllib3
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

# Подавление предупреждений SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Инициализация MIME-типов
mimetypes.init()

# Константы производительности
DEFAULT_CONCURRENCY = 150  # Оптимальное значение для большинства случаев
MAX_POOL_CONNECTIONS = 100  # Пул соединений
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
MAX_FILE_SIZE_IN_MEMORY = 10 * 1024 * 1024  # 10 MB - лимит для памяти


class S3Syncer:
    """Высокопроизводительный синхронизатор S3"""

    def __init__(self):
        load_dotenv()

        self.interrupted = False
        self._setup_signal_handlers()
        self._validate_env()

        # Настройки
        self.source_bucket = os.getenv('SOURCE_BUCKET_NAME')
        self.target_bucket = os.getenv('TARGET_BUCKET_NAME')
        self.concurrency = int(os.getenv('MAX_WORKERS', DEFAULT_CONCURRENCY))

        # Конфигурация с пулом соединений
        # Автоматически подстраиваем под MAX_WORKERS
        pool_size = min(self.concurrency, MAX_POOL_CONNECTIONS)
        self.aio_config = AioConfig(
            max_pool_connections=pool_size,
            connect_timeout=30,
            read_timeout=60,
        )

        # Конфигурации клиентов
        self.source_config = self._build_config('SOURCE')
        self.target_config = self._build_config('TARGET')

        # Статистика
        self.stats = {'total': 0, 'copied': 0, 'skipped': 0, 'errors': 0}

        # Семафор для ограничения параллельных операций
        self.semaphore = None

        # Сессия aioboto3
        self.session = aioboto3.Session()

    def _setup_signal_handlers(self):
        """Настройка обработчиков сигналов"""
        def handler(signum, frame):
            if not self.interrupted:
                self.interrupted = True
                print("\n\n⚠️  Прерывание... Завершаю текущие операции...")
            else:
                print("\n❌ Принудительная остановка!")
                sys.exit(130)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _validate_env(self):
        """Проверка переменных окружения"""
        required = [
            'SOURCE_AWS_ACCESS_KEY_ID', 'SOURCE_AWS_SECRET_ACCESS_KEY',
            'SOURCE_BUCKET_NAME', 'TARGET_AWS_ACCESS_KEY_ID',
            'TARGET_AWS_SECRET_ACCESS_KEY', 'TARGET_BUCKET_NAME'
        ]
        missing = [v for v in required if not os.getenv(v)]
        if missing:
            raise ValueError(f"Отсутствуют: {', '.join(missing)}")

    def _build_config(self, prefix: str) -> dict:
        """Создание конфигурации клиента"""
        config = {
            'aws_access_key_id': os.getenv(f'{prefix}_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv(
                f'{prefix}_AWS_SECRET_ACCESS_KEY'
            ),
            'config': self.aio_config,
        }

        if os.getenv(f'{prefix}_AWS_REGION'):
            config['region_name'] = os.getenv(f'{prefix}_AWS_REGION')
        if os.getenv(f'{prefix}_ENDPOINT_URL'):
            config['endpoint_url'] = os.getenv(f'{prefix}_ENDPOINT_URL')
        if os.getenv(f'{prefix}_VERIFY_SSL', 'true').lower() == 'false':
            config['verify'] = False

        return config

    async def get_all_objects(self) -> List[Dict]:
        """Получение списка всех объектов (асинхронно)"""
        objects = []

        print(f"📋 Получение списка файлов из {self.source_bucket}...")

        async with self.session.client('s3', **self.source_config) as client:
            paginator = client.get_paginator('list_objects_v2')

            async for page in paginator.paginate(Bucket=self.source_bucket):
                if 'Contents' in page:
                    objects.extend(page['Contents'])

                if self.interrupted:
                    break

        print(f"✅ Найдено файлов: {len(objects):,}")
        return objects

    async def check_target_exists(
        self,
        client,
        key: str,
        source_size: int
    ) -> Tuple[bool, Optional[str]]:
        """
        Проверка существования файла в целевом бакете

        Returns:
            Tuple[bool, Optional[str]]: (размер_совпадает, content_type)
        """
        try:
            response = await client.head_object(
                Bucket=self.target_bucket,
                Key=key
            )
            size_matches = response['ContentLength'] == source_size
            current_content_type = response.get('ContentType', '')
            return (size_matches, current_content_type)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return (False, None)
            raise

    async def copy_single_object(
        self,
        source_client,
        target_client,
        obj: Dict
    ) -> Tuple[str, str]:
        """Копирование одного объекта"""
        if self.interrupted:
            return (obj['Key'], 'interrupted')

        key = obj['Key']
        source_size = obj['Size']

        async with self.semaphore:
            try:
                # Определяем правильный MIME-тип по расширению
                correct_type, _ = mimetypes.guess_type(key)
                correct_type = (correct_type or 'application/octet-stream')

                # Проверка существования и получение текущего MIME-типа
                size_ok, current_type = await self.check_target_exists(
                    target_client, key, source_size
                )

                # Пропускаем только если размер И MIME-тип правильные
                if size_ok and current_type:
                    # Нормализуем для сравнения
                    current_normalized = current_type.lower().strip()
                    correct_normalized = correct_type.lower().strip()

                    # Проверяем совпадение MIME-типа
                    if current_normalized == correct_normalized:
                        return (key, 'skipped')
                    # Если MIME не совпадает - перезапишем с правильным

                # Защита от больших файлов в памяти
                if source_size > MAX_FILE_SIZE_IN_MEMORY:
                    return await self._copy_large_file(
                        source_client,
                        target_client,
                        key,
                        correct_type
                    )

                # Для маленьких файлов - обычное копирование
                response = await source_client.get_object(
                    Bucket=self.source_bucket,
                    Key=key
                )

                # Читаем содержимое
                body = await response['Body'].read()

                # Приоритет: правильный MIME из расширения
                content_type = correct_type

                # Загружаем в целевой бакет с правильным MIME
                await target_client.put_object(
                    Bucket=self.target_bucket,
                    Key=key,
                    Body=body,
                    ContentType=content_type,
                    Metadata=response.get('Metadata', {})
                )

                # Очистка
                del body

                return (key, 'copied')

            except ClientError as e:
                return (key, f"error: {e.response['Error']['Code']}")
            except Exception as e:
                return (key, f"error: {str(e)}")

    async def _copy_large_file(
        self,
        source_client,
        target_client,
        key: str,
        content_type: str
    ) -> Tuple[str, str]:
        """Копирование больших файлов (>10MB)"""
        try:
            response = await source_client.get_object(
                Bucket=self.source_bucket,
                Key=key
            )
            body = await response['Body'].read()

            await target_client.put_object(
                Bucket=self.target_bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                Metadata=response.get('Metadata', {})
            )

            del body
            return (key, 'copied')
        except Exception as e:
            return (key, f"error: {str(e)}")

    async def process_batch(
        self,
        source_client,
        target_client,
        objects: List[Dict],
        pbar
    ) -> None:
        """Обработка батча объектов через asyncio.gather"""
        tasks = [
            self.copy_single_object(source_client, target_client, obj)
            for obj in objects
        ]

        # Запускаем все задачи параллельно!
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.stats['errors'] += 1
                tqdm.write(f"❌ Exception: {result}")
            else:
                key, status = result
                if status == 'copied':
                    self.stats['copied'] += 1
                elif status == 'skipped':
                    self.stats['skipped'] += 1
                elif status == 'interrupted':
                    pass
                else:
                    self.stats['errors'] += 1
                    tqdm.write(f"❌ {status}")

            pbar.update(1)

            if self.interrupted:
                break

    async def sync(self):
        """Основной метод синхронизации"""
        # Создаем семафор
        self.semaphore = asyncio.Semaphore(self.concurrency)

        print("🚀 Начало синхронизации")
        print(f"📤 Источник: {self.source_bucket}")
        print(f"📥 Назначение: {self.target_bucket}")
        print(f"⚡ Параллельных операций: {self.concurrency}\n")

        # Получаем список объектов
        objects = await self.get_all_objects()

        if not objects:
            print("ℹ️  Нет файлов для копирования")
            return

        self.stats['total'] = len(objects)

        print("\n📦 Копирование файлов...")

        # Открываем оба клиента один раз
        async with self.session.client('s3', **self.source_config) as src:
            async with self.session.client('s3', **self.target_config) as tgt:

                # Обрабатываем батчами для лучшего управления памятью
                # Батч = MAX_WORKERS × 3 для баланса памяти/скорости
                batch_size = self.concurrency * 3

                with tqdm(total=len(objects), unit='файл') as pbar:
                    for i in range(0, len(objects), batch_size):
                        if self.interrupted:
                            break

                        batch = objects[i:i + batch_size]
                        await self.process_batch(src, tgt, batch, pbar)

        self._print_summary()

    def _print_summary(self):
        """Вывод итоговой статистики"""
        print("\n" + "=" * 60)
        print("📊 ИТОГИ СИНХРОНИЗАЦИИ")
        print("=" * 60)
        print(f"Всего файлов:      {self.stats['total']:,}")
        print(f"✅ Скопировано:    {self.stats['copied']:,}")
        print(f"⏭️  Пропущено:      {self.stats['skipped']:,}")
        print(f"❌ Ошибок:         {self.stats['errors']:,}")

        if self.interrupted:
            processed = sum([
                self.stats['copied'],
                self.stats['skipped'],
                self.stats['errors']
            ])
            remaining = self.stats['total'] - processed
            if remaining > 0:
                print(f"⏸️  Не обработано:  {remaining:,}")

        print("=" * 60 + "\n")

        if self.interrupted:
            print("⚠️  Синхронизация прервана")
        elif self.stats['errors'] > 0:
            print("⚠️  Завершено с ошибками")
        else:
            print("🎉 Синхронизация успешно завершена!")


async def main():
    """Точка входа"""
    try:
        syncer = S3Syncer()
        await syncer.sync()
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
