<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

class Notification
{
    public function __construct(
        public int $pid,
        public string $channel,
        public string $payload,
    ) {
    }
}
