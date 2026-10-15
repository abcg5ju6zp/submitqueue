-- speculation_receipt holds a head speculation's immutable conditional-stage receipts.
-- A receipt is written before its stage's message is published, so recovery replays
-- the publish under the stored message_id instead of enacting the stage a second time.
-- queue leads the PK so the table is shardable by queue; the remaining parts name one
-- stage action: the head batch, the stage, and the generation token (committed batch
-- version for land-dispatch, build ID for build-signal). Rows are write-once.
--
-- The InnoDB utf8mb4 index limit is 3072 bytes. queue and head follow the house
-- 255-character convention (1020 bytes each); stage is a short closed vocabulary
-- (128 bytes), so token is capped at 191 characters (764 bytes), leaving the
-- four-column key at 2932 bytes. The tokens stored here — a batch version number
-- or a runner build ID — are far shorter in every supported runner.
CREATE TABLE IF NOT EXISTS speculation_receipt (
    queue VARCHAR(255) NOT NULL,
    head VARCHAR(255) NOT NULL,
    stage VARCHAR(32) NOT NULL,
    token VARCHAR(191) NOT NULL,
    message_id VARCHAR(512) NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (queue, head, stage, token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
