<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Internal;

use InvalidArgumentException;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\ConfigParseException;
use PhpPg\PgConn\Config\Parser\ConfigParser;

use function ctype_digit;
use function inet_pton;
use function str_contains;
use function str_starts_with;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function trim;

/**
 * @see https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
 *
 * @param string $connString
 * @return Config
 *
 * @throws ConfigParseException
 */
function parseConfig(string $connString): Config
{
    return ConfigParser::parse($connString);
}

/**
 * Golang port of net.SplitHostPort function
 *
 * SplitHostPort splits a network address of the form "host:port",
 * "host%zone:port", "[host]:port" or "[host%zone]:port" into host or
 * host%zone and port.
 *
 * A literal IPv6 address in hostWithPort must be enclosed in square
 * brackets, as in "[::1]:80", "[::1%lo0]:80".
 *
 * See func Dial for a description of the hostWithPort parameter, and host
 * and port results.
 * @param string $hostWithPort
 * @return array{host: string, port: string}
 *
 * @throws InvalidArgumentException
 */
function splitHostAndPort(string $hostWithPort): array
{
    $portDelimIdx = strrpos($hostWithPort, ':');
    if ($portDelimIdx === false) {
        throw new InvalidArgumentException("Missing port in address");
    }

    // IPv6 parse
    if ($hostWithPort[0] === '[') {
        $end = strpos($hostWithPort, ']');
        if ($end === false) {
            throw new InvalidArgumentException("Missing ']' in address");
        }

        switch ($end + 1) {
            case strlen($hostWithPort):
                throw new InvalidArgumentException("Missing port in address");

            case $portDelimIdx:
                // ok
                break;

            default:
                if ($hostWithPort[$end + 1] === ':') {
                    throw new InvalidArgumentException("Too many colons in address");
                }

                throw new InvalidArgumentException("Missing port in address");
        }

        $host = substr($hostWithPort, 1, $end - 1);
    } else {
        $host = substr($hostWithPort, 0, $portDelimIdx);
        if (str_contains($host, ':')) {
            throw new InvalidArgumentException("Too many colons in address");
        }
    }

    $port = substr($hostWithPort, $portDelimIdx + 1);

    return ['host' => $host, 'port' => $port];
}

function isIPOnly(string $host): bool
{
    return inet_pton(trim($host, '[]')) !== false && !str_contains($host, ':');
}

/**
 * @param string $port
 * @return int
 *
 * @throws InvalidArgumentException
 */
function parsePort(string $port): int
{
    if (!ctype_digit($port)) {
        throw new InvalidArgumentException('Port must be a numeric value');
    }

    $portNum = (int)$port;
    if ($port < 1 || $port > 65535) {
        throw new InvalidArgumentException('Port number must be between 1 and 65535');
    }

    return $portNum;
}

/**
 * @param string $host
 * @param int $port
 * @return array{network: string, address:string}
 */
function getNetworkAddress(string $host, int $port): array
{
    if (str_starts_with($host, "/")) {
        return ['network' => 'unix', 'address' => "{$host}.s.PGSQL.{$port}"];
    }

    return ['network' => 'tcp', 'address' => "{$host}:{$port}"];
}
