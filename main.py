#!/usr/bin/env python3
"""
S3 Sync Script - копирование файлов между S3 бакетами без локального сохранения
Поддержка разных AWS аккаунтов, многопоточность, проверка размера файлов
"""

import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm


class S3Syncer:
    """Класс для синхронизации файлов между S3 бакетами"""

    def __init__(self):
        """Инициализация клиентов S3 и настроек"""
        # Загрузка переменных окружения
        load_dotenv()

        # Проверка наличия всех необходимых переменных
        required_vars = [
            'SOURCE_AWS_ACCESS_KEY_ID', 'SOURCE_AWS_SECRET_ACCESS_KEY',
            'SOURCE_BUCKET_NAME',
            'TARGET_AWS_ACCESS_KEY_ID', 'TARGET_AWS_SECRET_ACCESS_KEY',
            'TARGET_BUCKET_NAME'
        ]

        missing_vars = [
            var for var in required_vars if not os.getenv(var)
        ]
        if missing_vars:
            raise ValueError(
                f"Отсутствуют переменные окружения: "
                f"{', '.join(missing_vars)}"
            )

        # Создание клиента для исходного S3
        source_config = {
            'aws_access_key_id': os.getenv('SOURCE_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv(
                'SOURCE_AWS_SECRET_ACCESS_KEY'
            ),
        }

        # Опциональные параметры для источника
        if os.getenv('SOURCE_AWS_REGION'):
            source_config['region_name'] = os.getenv('SOURCE_AWS_REGION')
        if os.getenv('SOURCE_ENDPOINT_URL'):
            source_config['endpoint_url'] = os.getenv('SOURCE_ENDPOINT_URL')
        if os.getenv('SOURCE_VERIFY_SSL', 'true').lower() == 'false':
            source_config['verify'] = False

        # S3 addressing style (path/virtual)
        if os.getenv('SOURCE_ADDRESSING_STYLE'):
            source_config['config'] = boto3.session.Config(
                s3={'addressing_style': os.getenv('SOURCE_ADDRESSING_STYLE')}
            )

        self.source_client = boto3.client('s3', **source_config)

        # Создание клиента для целевого S3
        target_config = {
            'aws_access_key_id': os.getenv('TARGET_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv(
                'TARGET_AWS_SECRET_ACCESS_KEY'
            ),
        }

        # Опциональные параметры для назначения
        if os.getenv('TARGET_AWS_REGION'):
            target_config['region_name'] = os.getenv('TARGET_AWS_REGION')
        if os.getenv('TARGET_ENDPOINT_URL'):
            target_config['endpoint_url'] = os.getenv('TARGET_ENDPOINT_URL')
        if os.getenv('TARGET_VERIFY_SSL', 'true').lower() == 'false':
            target_config['verify'] = False

        # S3 addressing style (path/virtual)
        if os.getenv('TARGET_ADDRESSING_STYLE'):
            target_config['config'] = boto3.session.Config(
                s3={'addressing_style': os.getenv('TARGET_ADDRESSING_STYLE')}
            )

        self.target_client = boto3.client('s3', **target_config)

        self.source_bucket = os.getenv('SOURCE_BUCKET_NAME')
        self.target_bucket = os.getenv('TARGET_BUCKET_NAME')
        self.max_workers = int(os.getenv('MAX_WORKERS', '10'))

        # Статистика
        self.stats = {
            'total': 0,
            'copied': 0,
            'skipped': 0,
            'errors': 0
        }

    def get_all_objects(self) -> list:
        """
        Получение списка всех объектов из исходного бакета
        с пагинацией

        Returns:
            list: Список словарей с информацией об объектах
        """
        objects = []

        try:
            paginator = self.source_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.source_bucket)

            source_bucket_msg = (
                f"📋 Получение списка файлов из бакета "
                f"{self.source_bucket}..."
            )
            print(source_bucket_msg)

            for page in page_iterator:
                if 'Contents' in page:
                    objects.extend(page['Contents'])

            print(f"✅ Найдено файлов: {len(objects)}")

        except ClientError as e:
            print(f"❌ Ошибка при получении списка объектов: {e}")
            raise

        return objects

    def check_target_object(self, key: str) -> Optional[int]:
        """
        Проверка существования объекта в целевом бакете и получение
        его размера

        Args:
            key: Ключ объекта

        Returns:
            Optional[int]: Размер файла в байтах или None если файл
                          не существует
        """
        try:
            response = self.target_client.head_object(
                Bucket=self.target_bucket,
                Key=key
            )
            return response['ContentLength']
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            else:
                # Другая ошибка - пробрасываем дальше
                raise

    def copy_object(self, obj: Dict) -> Tuple[str, str]:
        """
        Копирование объекта из исходного бакета в целевой через память

        Args:
            obj: Словарь с информацией об объекте

        Returns:
            Tuple[str, str]: (ключ объекта, статус:
                             'copied'/'skipped'/'error')
        """
        key = obj['Key']
        source_size = obj['Size']

        try:
            # Проверка существования в целевом бакете
            target_size = self.check_target_object(key)

            # Если файл существует и размер совпадает - пропускаем
            if target_size is not None and target_size == source_size:
                return (key, 'skipped')

            # Скачивание объекта в память
            response = self.source_client.get_object(
                Bucket=self.source_bucket,
                Key=key
            )

            # Чтение содержимого в BytesIO
            file_content = io.BytesIO(response['Body'].read())
            file_content.seek(0)

            # Загрузка в целевой бакет
            self.target_client.upload_fileobj(
                file_content,
                self.target_bucket,
                key
            )

            return (key, 'copied')

        except ClientError as e:
            error_msg = f"{key}: {e.response['Error']['Code']}"
            return (key, f'error: {error_msg}')
        except Exception as e:
            return (key, f'error: {str(e)}')

    def sync(self):
        """Основной метод синхронизации"""
        print("🚀 Начало синхронизации")

        # Формирование информации об источнике
        source_endpoint = os.getenv('SOURCE_ENDPOINT_URL', 'AWS S3')
        source_region = os.getenv('SOURCE_AWS_REGION', 'default')
        source_msg = (
            f"📤 Источник: {self.source_bucket} "
            f"({source_endpoint}, {source_region})"
        )

        # Формирование информации о назначении
        target_endpoint = os.getenv('TARGET_ENDPOINT_URL', 'AWS S3')
        target_region = os.getenv('TARGET_AWS_REGION', 'default')
        target_msg = (
            f"📥 Назначение: {self.target_bucket} "
            f"({target_endpoint}, {target_region})"
        )

        print(source_msg)
        print(target_msg)
        print(f"🔧 Потоков: {self.max_workers}\n")

        # Получение списка всех объектов
        objects = self.get_all_objects()

        if not objects:
            print("ℹ️  Нет файлов для копирования")
            return

        self.stats['total'] = len(objects)

        # Многопоточное копирование с прогресс-баром
        print("\n📦 Копирование файлов...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Запуск задач
            futures = {
                executor.submit(self.copy_object, obj): obj
                for obj in objects
            }

            # Обработка результатов с прогресс-баром
            with tqdm(total=len(objects), unit='файл', ncols=100) as pbar:
                for future in as_completed(futures):
                    key, status = future.result()

                    if status == 'copied':
                        self.stats['copied'] += 1
                    elif status == 'skipped':
                        self.stats['skipped'] += 1
                    elif status.startswith('error'):
                        self.stats['errors'] += 1
                        # Логируем только ошибки
                        tqdm.write(f"❌ {status}")

                    pbar.update(1)

        # Итоговая статистика
        self._print_summary()

    def _print_summary(self):
        """Вывод итоговой статистики"""
        print("\n" + "="*60)
        print("📊 ИТОГИ СИНХРОНИЗАЦИИ")
        print("="*60)
        print(f"Всего файлов:      {self.stats['total']}")
        print(f"✅ Скопировано:    {self.stats['copied']}")
        skipped_msg = (
            f"⏭️  Пропущено:      {self.stats['skipped']} "
            "(уже существуют, размер совпадает)"
        )
        print(skipped_msg)
        print(f"❌ Ошибок:         {self.stats['errors']}")
        print("="*60 + "\n")

        if self.stats['errors'] > 0:
            print("⚠️  Синхронизация завершена с ошибками")
        else:
            print("🎉 Синхронизация успешно завершена!")


def main():
    """Точка входа в программу"""
    try:
        syncer = S3Syncer()
        syncer.sync()
    except ValueError as e:
        print(f"❌ Ошибка конфигурации: {e}")
        print("\nСоздайте файл .env по примеру .env.example")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
