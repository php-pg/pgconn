<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

// phpcs:ignoreFile
// since PHPCS does not support enums
enum SslMode: string
{
    case DISABLE = 'disable';
    case ALLOW = 'allow';
    case PREFER = 'prefer';
    case REQUIRE = 'require';
    case VERIFY_FULL = 'verify-full';
    case VERIFY_CA = 'verify-ca';

    public function isServerAllowedToRefuseTls(): bool
    {
        return match ($this) {
            self::DISABLE, self::ALLOW, self::PREFER => true,
            default => false,
        };
    }
}
