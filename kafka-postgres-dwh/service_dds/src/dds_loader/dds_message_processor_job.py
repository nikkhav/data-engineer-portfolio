from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(
            self,
            consumer: KafkaConsumer,
            producer: KafkaProducer,
            dds_repository: DdsRepository,
            logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30
        self._load_src = "kafka_dds"

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

            order_id = msg["object_id"]
            payload = msg['payload']

            # Загрузка хабов, линков и сателлитов
            # h_category
            products = payload.get("products", [])
            categories = list({p.get("category") for p in products if p.get("category")})

            category_pk_dict = {}

            for category in categories:
                category_pk_dict[category] = self._dds_repository.insert_hub(
                    hub_table="h_category",
                    bk_value=category,
                    load_src=self._load_src
                )

            # h_order
            order_date = payload.get("date")
            h_order_pk = self._dds_repository.insert_hub(hub_table="h_order", bk_value=order_id, load_src=self._load_src, order_dt=order_date)

            # h_restaurant
            restaurant_data = payload.get("restaurant", {})
            restaurant_id = restaurant_data.get("id")
            h_restaurant_pk = self._dds_repository.insert_hub(hub_table="h_restaurant", bk_value=restaurant_id, load_src=self._load_src)

            # h_user
            user_data = payload.get("user", {})
            user_id = user_data.get("id")
            h_user_pk = self._dds_repository.insert_hub(hub_table="h_user", bk_value=user_id, load_src=self._load_src)

            # h_product + l_order_product + l_product_category + l_product_restaurant + s_product_names
            for product in products:
                product_id = product.get("id")
                category_name = product.get("category")
                product_name = product.get("name", "")

                # 1) HUB PRODUCT
                h_product_pk = self._dds_repository.insert_hub(
                    hub_table="h_product",
                    bk_value=product_id,
                    load_src=self._load_src
                )

                # 2) LINK l_order_product
                self._dds_repository.insert_link(
                    link_table="l_order_product",
                    hub_keys={
                        "h_order_pk": h_order_pk,
                        "h_product_pk": h_product_pk
                    },
                    load_src=self._load_src
                )

                # 3) LINK l_product_category
                if category_name:
                    h_category_pk = category_pk_dict[category_name]

                    self._dds_repository.insert_link(
                        link_table="l_product_category",
                        hub_keys={
                            "h_product_pk": h_product_pk,
                            "h_category_pk": h_category_pk
                        },
                        load_src=self._load_src
                    )

                # 4) LINK l_product_restaurant
                self._dds_repository.insert_link(
                    link_table="l_product_restaurant",
                    hub_keys={
                        "h_product_pk": h_product_pk,
                        "h_restaurant_pk": h_restaurant_pk
                    },
                    load_src=self._load_src
                )

                # 5) SATELLITE s_product_names
                self._dds_repository.insert_satellite(
                    sat_table="s_product_names",
                    hub_pk=h_product_pk,
                    payload={
                        "name": product_name,
                    },
                    load_src=self._load_src,
                )

                # 6) Отправляем сообщение в следующий топик - счётчик по продуктам
                self._producer.produce({
                    "object_id": order_id,
                    "object_type": "product_counter",
                    "payload": {
                        "user_id": str(h_user_pk),
                        "product_id": str(h_product_pk),
                        "product_name": product.get("name", ""),
                        "count": 1
                    }
                })
                self._logger.info(
                    f"{datetime.now(timezone.utc)}. Product counter message sent. Product ID: {product_id}")


            # l_order_user
            self._dds_repository.insert_link(
                link_table="l_order_user",
                hub_keys={
                    "h_order_pk": h_order_pk,
                    "h_user_pk": h_user_pk
                },
                load_src=self._load_src
            )

            # s_order_cost
            cost = payload.get("cost", "")
            payment = payload.get("payment", "")

            self._dds_repository.insert_satellite(
                sat_table="s_order_cost",
                hub_pk=h_order_pk,
                payload={
                    "cost": cost,
                    "payment": payment,
                },
                load_src=self._load_src,
            )

            # s_order_status
            status = payload.get("status", "")
            self._dds_repository.insert_satellite(
                sat_table="s_order_status",
                hub_pk=h_order_pk,
                payload={
                    "status": status,
                },
                load_src=self._load_src,
            )

            # s_restaurant_names
            restaurant_name = restaurant_data.get("name", "")
            self._dds_repository.insert_satellite(
                sat_table="s_restaurant_names",
                hub_pk=h_restaurant_pk,
                payload={
                    "name": restaurant_name,
                },
                load_src=self._load_src,
            )

            # s_user_names
            user_name = user_data.get("name", "")
            user_login = user_data.get("login", "")

            self._dds_repository.insert_satellite(
                sat_table="s_user_names",
                hub_pk=h_user_pk,
                payload={
                    "username": user_name,
                    "userlogin": user_login,
                },
                load_src=self._load_src,
            )

            self._logger.info(f"{datetime.now(timezone.utc)}. Order inserted in DDS. Order ID: {order_id}")

            # Отправим счётчик заказов по категориям
            for product in products:
                category_name = product.get("category")
                if category_name:
                    self._producer.produce({
                        "object_id": order_id,
                        "object_type": "category_counter",
                        "payload": {
                            "user_id": str(h_user_pk),
                            "category_id": str(category_pk_dict[category_name]),
                            "category_name": category_name,
                            "count": 1
                        }
                    })
                    self._logger.info(
                        f"{datetime.now(timezone.utc)}. Category counter message sent. Category ID: {str(category_pk_dict[category_name])}")


        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
