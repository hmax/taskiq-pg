CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    task_status VARCHAR NOT NULL DEFAULT 'pending',
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    notified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);
"""

INSERT_MESSAGE_QUERY = """
INSERT INTO {} (task_id, task_name, task_status, message, labels)
VALUES ($1, $2, $3, $4, $5)
RETURNING id
"""

ASSIGN_MESSAGE_QUERY = ("UPDATE {} SET task_status='assigned', assigned_at = NOW() "
                        "WHERE id = $1 AND task_status = 'pending' RETURNING *")

UPDATE_OLD_PENDING_MESSAGES_QUERY = ("UPDATE {} SET notified_at = NOW() "
                                     "WHERE task_status = 'pending' "
                                     "AND NOW() - notified_at > INTERVAL '{} seconds' "
                                     "RETURNING id")

MOVE_OLD_ASSIGNED_TASKS_TO_PENDING_QUERY = """
UPDATE {} SET task_status = 'pending', assigned_at = NULL
WHERE task_status = 'assigned'
AND NOW() - assigned_at > INTERVAL '{} seconds'"""

DELETE_MESSAGE_QUERY = "DELETE FROM {} WHERE id = $1 AND task_status = 'assigned'"
