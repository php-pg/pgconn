<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use InvalidArgumentException;

use function array_key_exists;
use function count;
use function explode;
use function implode;
use function parse_str;
use function parse_url;
use function PhpPg\PgConn\Config\Internal\isIPOnly;
use function PhpPg\PgConn\Config\Internal\splitHostAndPort;

class UriParser
{
    private const NAME_MAP = [
        'dbname' => 'database',
    ];

    /**
     * @param string $connString
     * @return array<string, string>
     *
     * @throws InvalidArgumentException
     */
    public static function parse(string $connString): array
    {
        $settings = [];

        $url = parse_url($connString);
        if ($url === false) {
            throw new InvalidArgumentException("Cannot parse connection string url: {$connString}");
        }

        if (array_key_exists('user', $url)) {
            $settings['user'] = $url['user'];

            if (array_key_exists('pass', $url)) {
                $settings['password'] = $url['pass'];
            }
        }

        $hosts = [];
        $ports = [];

        if (array_key_exists('host', $url)) {
            // Handle multiple host:port's by splitting them into host,host,host and port,port,port.
            $hostWithPorts = explode(',', $url['host']);
            $hostWithPortsCnt = count($hostWithPorts);

            // parse_url behavior, port for last host will be set in 'port' string
            if (array_key_exists('port', $url)) {
                $hostWithPorts[$hostWithPortsCnt - 1] .= ':' . $url['port'];
                unset($url['port']);
            }

            foreach ($hostWithPorts as $hostWithPort) {
                if (isIPOnly($hostWithPort)) {
                    $hosts[] = trim($hostWithPort, '[]');
                    continue;
                }

                try {
                    ['host' => $host, 'port' => $port] = splitHostAndPort($hostWithPort);
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException("Cannot split host with port on {$hostWithPort}", 0, $e);
                }

                $hosts[] = $host;
                $ports[] = $port;
            }
        }

        if ($hosts !== []) {
            $settings['host'] = implode(',', $hosts);
        }

        if ($ports !== []) {
            $settings['port'] = implode(',', $ports);
        }

        if (array_key_exists('path', $url)) {
            $database = ltrim($url['path'], '/');
            if ($database !== '') {
                $settings['database'] = $database;
            }
        }

        if (array_key_exists('query', $url)) {
            $queryVars = [];
            parse_str($url['query'], $queryVars);

            foreach ($queryVars as $key => $value) {
                $settings[self::NAME_MAP[$key] ?? $key] = $value;
            }
        }

        /** @var array<string, string> PHPStan */
        return $settings;
    }
}
