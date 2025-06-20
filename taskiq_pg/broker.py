import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
)

import asyncpg
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage
from typing_extensions import override

from taskiq_pg.broker_queries import (
    ASSIGN_MESSAGE_QUERY,
    CREATE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
    MOVE_OLD_ASSIGNED_TASKS_TO_PENDING_QUERY,
    UPDATE_OLD_PENDING_MESSAGES_QUERY,
)

_T = TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: Union[
            str, Callable[[], str],
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        max_execution_time_seconds: int = 120,
        stuck_tasks_check_period: int = 60,
        connection_kwargs: Optional[dict[str, Any]] = None,
        pool_kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Construct a new broker.

        :param dsn: connection string to PostgreSQL, or callable returning one.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param max_execution_time_seconds: Maximum execution time for a task.
        If the task takes longer, it will be reassigned.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        :param pool_kwargs: Additional arguments for asyncpg pool creation.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: Union[str, Callable[[], str]] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        self.max_execution_time_seconds: int = max_execution_time_seconds
        self.stuck_tasks_check_period: int = stuck_tasks_check_period
        self.read_conn: Optional["asyncpg.Connection[asyncpg.Record]"] = None
        self.write_pool: Optional["asyncpg.pool.Pool[asyncpg.Record]"] = None
        self._queue: Optional[asyncio.Queue[str]] = None

    @property
    def dsn(self) -> str:
        """Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    @override
    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        self.read_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.pool_kwargs)

        if self.read_conn is None:
            msg = "read_conn not initialized"
            raise RuntimeError(msg)
        if self.write_pool is None:
            msg = "write_pool not initialized"
            raise RuntimeError(msg)

        async with self.write_pool.acquire() as conn:
            _ = await conn.execute(CREATE_TABLE_QUERY.format(self.table_name))

        if self.is_worker_process:
            _ = asyncio.create_task(self._check_stuck_tasks())   # noqa: RUF006

        await self.read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

    @override
    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.read_conn is not None:
            await self.read_conn.close()
        if self.write_pool is not None:
            await self.write_pool.close()

    def _notification_handler(
        self,
        con_ref: Union[
            "asyncpg.Connection[asyncpg.Record]",
            "asyncpg.pool.PoolConnectionProxy[asyncpg.Record]",
        ],
        pid: int,
        channel: str,
        payload: object,
        /,
    ) -> None:
        """Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **con_ref**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        logger.debug(f"Received notification on channel {channel}: {payload}")
        if self._queue is not None:
            self._queue.put_nowait(str(payload))

    @override
    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before kicking.")

        async with self.write_pool.acquire() as conn:
            # Insert the message into the database
            message_inserted_id = cast(
                int,
                await conn.fetchval(
                    INSERT_MESSAGE_QUERY.format(self.table_name),
                    message.task_id,
                    message.task_name,
                    "pending",
                    message.message.decode(),
                    json.dumps(message.labels),
                ),
            )

            delay_value = message.labels.get("delay")
            if delay_value is not None:
                delay_seconds = int(delay_value)
                _ = asyncio.create_task(  # noqa: RUF006
                    self._schedule_notification(message_inserted_id, delay_seconds),
                )
            else:
                # Send a NOTIFY with the message ID as payload
                _ = await conn.execute(
                    f"NOTIFY {self.channel_name}, '{message_inserted_id}'",
                )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self.write_pool is None:
            return
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY
            _ = await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def _check_stuck_tasks(self) -> None:
        """Rekick orphaned tasks that were not acknowledged."""
        if self.write_pool is None:
            raise ValueError("Please run startup before rekicking orphaned tasks.")

        logger.info("Starting periodic check for orphaned tasks")
        while True:
            try:
                message_ids = await self._reassign_orphaned_tasks_db()

                if message_ids:
                    logger.info(f"Re-notifying {len(message_ids)} "
                                f"pending tasks: {message_ids}")
                    notification_query = ";".join(
                        f"NOTIFY {self.channel_name}, '{message_id}'"
                        for message_id in message_ids
                    )

                    async with self.write_pool.acquire() as conn:
                        await conn.execute(notification_query)

            except Exception as exc:
                logger.error(f"Error during a stuck tasks check: {exc}", exc_info=True)

            await asyncio.sleep(self.stuck_tasks_check_period)

    async def _reassign_orphaned_tasks_db(self) -> list[int]:
        """
        Check for orphaned tasks in the database and reset them to a pending/assignable state.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before rekicking orphaned tasks.")

        async with self.write_pool.acquire() as conn, conn.transaction():
            _ = await conn.execute(
                MOVE_OLD_ASSIGNED_TASKS_TO_PENDING_QUERY.format(
                    self.table_name,
                    self.max_execution_time_seconds,
                ),
            )
            result = await conn.fetch(
                UPDATE_OLD_PENDING_MESSAGES_QUERY.format(
                    self.table_name,
                    self.max_execution_time_seconds,
                ),
            )

        return [row["id"] for row in result if row["id"] is not None]

    @override
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self.read_conn is None or self.write_pool is None:
            raise ValueError("Call startup before starting listening.")
        if self._queue is None:
            raise ValueError("Startup did not initialize the queue.")

        while True:
            try:
                payload = await self._queue.get()
                message_id = int(payload)
                message_row = await self.write_pool.fetchrow(
                    ASSIGN_MESSAGE_QUERY.format(self.table_name), message_id,
                )
                if message_row is None:
                    logger.warning(
                        f"Message with id {message_id} not found in database.",
                    )
                    continue
                if message_row.get("message") is None:
                    msg = "Message row does not have 'message' column"
                    raise ValueError(msg)
                message_str = message_row["message"]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise ValueError(msg)
                message_data = message_str.encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self.write_pool is None:
                        raise ValueError("Call startup before starting listening.")

                    async with self.write_pool.acquire() as conn:
                        _ = await conn.execute(
                            DELETE_MESSAGE_QUERY.format(self.table_name),
                            _message_id,
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                continue
