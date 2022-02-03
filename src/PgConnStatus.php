<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

// phpcs:ignoreFile
// since PHPCS does not support enums
enum PgConnStatus: int
{
    case IDLE = 1;
    case BUSY = 2;
    case CLOSED = 3;

    public static function isActive(PgConnStatus $status): bool
    {
        return $status === self::IDLE || $status === self::BUSY;
    }
}
