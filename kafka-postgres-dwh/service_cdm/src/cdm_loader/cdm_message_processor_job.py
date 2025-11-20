import uuid
from datetime import datetime, timezone
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
            self,
            consumer: KafkaConsumer,
            cdm_repository: CdmRepository,
            logger: Logger
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.error(f"{datetime.now(timezone.utc)}: No message received. Exiting batch processing.")
                break

            required = ("object_id", "object_type", "payload")
            if any(field not in msg for field in required):
                self._logger.warning(f"Missing required fields: {msg}. Skipping.")
                continue

            self._logger.info(f"{datetime.now(timezone.utc)}: Message received")

            object_type = msg["object_type"]
            payload = msg['payload']

            assert object_type in ["category_counter", "product_counter"], f"Unknown object_type: {object_type}"

            if object_type == "category_counter":
                self._cdm_repository.insert_user_category_counters(
                    user_id=uuid.UUID(payload["user_id"]),
                    category_id=uuid.UUID(payload["category_id"]),
                    category_name=payload["category_name"],
                    order_cnt=payload["count"],
                )
                self._logger.info(f"{datetime.now(timezone.utc)}. Category count incremented. Category id: {payload['category_id']}")

            elif object_type == "product_counter":
                self._cdm_repository.insert_user_product_counters(
                    user_id=uuid.UUID(payload["user_id"]),
                    product_id=uuid.UUID(payload["product_id"]),
                    product_name=payload["product_name"],
                    order_cnt=payload["count"],
                )
                self._logger.info(
                    f"{datetime.now(timezone.utc)}. Product count incremented: Product id: {payload['product_id']}")

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
