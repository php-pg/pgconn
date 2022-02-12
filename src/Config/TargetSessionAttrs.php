<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

// phpcs:ignoreFile
// since PHPCS does not support enums
enum TargetSessionAttrs: string
{
    case ANY = 'any';
    case READ_WRITE = 'read-write';
    case READ_ONLY = 'read-only';
    case PRIMARY = 'primary';
    case STANDBY = 'standby';
    case PREFER_STANDBY = 'prefer-standby';
}
