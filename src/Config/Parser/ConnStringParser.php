<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use function str_starts_with;

class ConnStringParser
{
    /**
     * @param string $connString
     * @return array<string, string>
     *
     * @throws \InvalidArgumentException
     */
    public static function parse(string $connString): array
    {
        if ($connString === '') {
            return [];
        }

        if (
            str_starts_with($connString, 'postgres://') ||
            str_starts_with($connString, 'postgresql://')
        ) {
            return UriParser::parse($connString);
        }

        return DsnParser::parse($connString);
    }
}
