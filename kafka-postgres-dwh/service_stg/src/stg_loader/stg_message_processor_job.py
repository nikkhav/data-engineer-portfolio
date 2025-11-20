import json
from logging import Logger
from typing import List, Dict
from datetime import datetime, timezone

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.error(f"{datetime.now(timezone.utc)}: No message received. Exiting batch processing.")
                break

            required = ("object_type", "object_id", "sent_dttm", "payload")
            if any(field not in msg for field in required):
                self._logger.warning(f"Missing required fields: {msg}. Skipping.")
                continue

            self._logger.info(f"{datetime.now(timezone.utc)}: Message received")

            object_id = msg["object_id"]
            object_type = msg["object_type"]
            sent_dttm = msg["sent_dttm"]
            order = msg['payload']

            self._stg_repository.order_events_insert(
                object_id,
                object_type,
                sent_dttm,
                json.dumps(order))

            user_id = order["user"]["id"]
            try:
                user = self._redis.get(user_id)
                user_name = user["name"]
                user_login = user["login"]
            except KeyError as e:
                self._logger.error(f"User data missing for user_id {user_id}: {e}. Skipping message.")
                continue

            restaurant_id = order['restaurant']['id']
            try:
                restaurant = self._redis.get(restaurant_id)
                restaurant_name = restaurant["name"]
            except KeyError as e:
                self._logger.error(f"Restaurant data missing for restaurant_id {restaurant_id}: {e}. Skipping message.")
                continue

            dst_msg = {
                "object_id": object_id,
                "object_type": object_type,
                "payload": {
                    "id": object_id,
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name, user_login),
                    "products": self._format_items(order["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.now(timezone.utc)}. Message Sent. Object ID: {object_id}")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name, login) -> Dict[str, str]:
        return {
            "id": id,
            "name": name,
            "login": login
        }

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
