<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use function getenv;

class EnvParser
{
    private const NAME_MAP = [
        "PGHOST" => "host",
        "PGPORT" => "port",
        "PGDATABASE" => "database",
        "PGUSER" => "user",
        "PGPASSWORD" => "password",
        "PGPASSFILE" => "passfile",
        "PGAPPNAME" => "application_name",
        "PGCONNECT_TIMEOUT" => "connect_timeout",
        "PGSSLMODE" => "sslmode",
        "PGSSLKEY" => "sslkey",
        "PGSSLCERT" => "sslcert",
        "PGSSLROOTCERT" => "sslrootcert",
        "PGTARGETSESSIONATTRS" => "target_session_attrs",
        "PGSERVICE" => "service",
        "PGSERVICEFILE" => "servicefile",
    ];

    /**
     * @return array<string, string>
     */
    public static function parse(): array
    {
        $settings = [];

        foreach (self::NAME_MAP as $envName => $realName) {
            if (false !== ($value = getenv($envName))) {
                $settings[$realName] = $value;
            }
        }

        /** @var array<string, string> PHPStan */
        return $settings;
    }
}
