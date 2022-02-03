<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Internal;

use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use InvalidArgumentException;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\ConfigParseException;
use PhpPg\PgConn\Config\FallbackConfig;
use PhpPg\PgConn\SslMode;
use PhpPg\PgPassFile\PgPassFile;
use PhpPg\PgServiceFile\PgServiceFile;

use function array_key_exists;
use function array_merge;
use function array_shift;
use function count;
use function ctype_digit;
use function explode;
use function file_exists;
use function get_current_user;
use function getenv;
use function implode;
use function ip2long;
use function is_numeric;
use function ltrim;
use function parse_str;
use function parse_url;
use function str_contains;
use function str_starts_with;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function trim;

/**
 * @param string $connString
 *
 * @return Config
 * @throws ConfigParseException
 */
function parseConfig(string $connString): Config
{
    $defaultSettings = getDefaultOptions();
    $envSettings = getEnvSettings();
    $connStringSettings = parseConnString($connString);

    $settings = array_merge($defaultSettings, $envSettings, $connStringSettings);

    // Parse service file
    if (
        array_key_exists('service', $settings) &&
        array_key_exists('servicefile', $settings)
    ) {
        try {
            $serviceSettings = getServiceSettings($settings['servicefile'], $settings['service']);
        } catch (InvalidArgumentException $e) {
            throw new ConfigParseException('Unable to parse service file', 0, $e);
        }

        $settings = array_merge($defaultSettings, $envSettings, $serviceSettings, $connStringSettings);
    }

    // TODO: Do something with target_session_attrs

    if (array_key_exists('connect_timeout', $settings)) {
        if (!is_numeric($settings['connect_timeout'])) {
            throw new ConfigParseException('connect_timeout must be a valid float number');
        }

        $connectTimeout = (float)$settings['connect_timeout'];
        if ($connectTimeout < 0.001) {
            throw new ConfigParseException('connect_timeout be greater than 0.001 seconds');
        }
    }

    static $nonRuntimeParameters = [
        "host" => null,
        "port" => null,
        "database" => null,
        "user" => null,
        "password" => null,
        "passfile" => null,
        "connect_timeout" => null,
        "sslmode" => null,
        "sslkey" => null,
        "sslcert" => null,
        "sslrootcert" => null,
        "target_session_attrs" => null,
        "min_read_buffer_size" => null,
        "service" => null,
        "servicefile" => null,
    ];

    $runtimeParameters = [];

    foreach ($settings as $key => $value) {
        if (array_key_exists($key, $nonRuntimeParameters)) {
            continue;
        }

        $runtimeParameters[$key] = $value;
    }

    $hosts = explode(',', $settings['host']);
    $ports = explode(',', $settings['port']);

    /** @var array<FallbackConfig> $fallbacks */
    $fallbacks = [];

    foreach ($hosts as $idx => $host) {
        $portStr = $ports[$idx] ?? $ports[0];

        try {
            $port = parsePort($portStr);
        } catch (InvalidArgumentException $e) {
            throw new ConfigParseException("Invalid port {$portStr}", 0, $e);
        }

        ['network' => $network,] = getNetworkAddress($host, $port);
        // Ignore TLS settings if Unix domain socket like libpq
        if ($network === 'unix') {
            $fallbacks[] = new FallbackConfig($host, $port, null);
            continue;
        }

        try {
            ['config' => $tlsConfig, 'sslMode' => $sslMode] = configTLS($settings, $host);
        } catch (InvalidArgumentException $e) {
            throw new ConfigParseException('TLS Config error', 0, $e);
        }

        $fallbacks[] = new FallbackConfig($host, $port, $tlsConfig, $sslMode);
    }

    $fallback = array_shift($fallbacks);

    // Parse Postgres password file
    if (array_key_exists('passfile', $settings) && ($settings['password'] ?? '') === '') {
        try {
            $passFile = PgPassFile::open($settings['passfile']);
        } catch (InvalidArgumentException $e) {
            throw new ConfigParseException('Unable to parse password file', 0, $e);
        }

        $password = $passFile->findPassword(
            $fallback->host,
            (string)$fallback->port,
            $settings['database'],
            $settings['user']
        );
        $settings['password'] = $password;
    }

    return new Config(
        host: $fallback->host,
        port: $fallback->port,
        database: $settings['database'],
        user: $settings['user'],
        password: $settings['password'],
        tlsConfig: $fallback->tlsConfig,
        sslMode: $fallback->sslMode,
        connectTimeout: $connectTimeout ?? 2.0,
        runtimeParams: $runtimeParameters,
        fallbacks: $fallbacks,
    );
}

/**
 * @return array<string, string>
 */
function getDefaultOptions(): array
{
    $platformDependentOptions = [];
    if (PHP_OS_FAMILY === 'Windows') {
        $platformDependentOptions = getWindowsDefaultOptions();
    } elseif (PHP_OS_FAMILY === 'Linux' || PHP_OS_FAMILY === 'Darwin') {
        $platformDependentOptions = getLinuxDefaultOptions();
    }

    // TODO: Implement config files parsing

    return [
        'host' => '127.0.0.1',
        'port' => '5432',
        'target_session_attrs' => 'any',
        'min_read_buffer_size' => '8192',
        ...$platformDependentOptions,
    ];
}

/**
 * @return array<string, string>
 */
function getWindowsDefaultOptions(): array
{
    $settings = [];

    $user = get_current_user();
    if ($user === '') {
        return $settings;
    }

    $settings['user'] = $user;

    if (false === ($homeDrive = getenv('HOMEDRIVE'))) {
        return $settings;
    }
    if (false === ($homePath = getenv('HOMEPATH'))) {
        return $settings;
    }
    if (false === ($appData = getenv('APPDATA'))) {
        return $settings;
    }

    $homePath = $homeDrive . $homePath;

    $passFile = "{$appData}\\postgresql\\pgpass.conf";
    if (file_exists($passFile)) {
        $settings['passfile'] = $passFile;
    }

    // TODO
    /**
     * The connection service file can be a per-user service file at ~/.pg_service.conf
     * or the location specified by the environment variable PGSERVICEFILE,
     * or it can be a system-wide file at
     * `pg_config --sysconfdir`/pg_service.conf or in the directory specified by the environment variable PGSYSCONFDIR.
     * If service definitions with the same name exist in the user and the system file, the user file takes precedence.
     */
    $serviceFile = "{$homePath}\\.pg_service.conf";
    if (file_exists($serviceFile)) {
        $settings['servicefile'] = $serviceFile;
    }

    $sslCert = "{$appData}\\postgresql\\postgresql.crt";
    $sslKey = "{$appData}\\postgresql\\postgresql.key";

    if (file_exists($sslCert) && file_exists($sslKey)) {
        // Both the cert and key must be present to use them, or do not use either
        $settings['sslcert'] = $sslCert;
        $settings['sslkey'] = $sslKey;
    }

    $sslRootCert = "{$appData}\\postgresql\\root.crt";
    if (file_exists($sslRootCert)) {
        $settings['sslrootcert'] = $sslRootCert;
    }

    return $settings;
}

/**
 * @return array<string, string>
 */
function getLinuxDefaultOptions(): array
{
    $settings = [];

    $user = get_current_user();
    if ($user === '') {
        return $settings;
    }

    $settings['user'] = $user;

    if (false === ($homePath = getenv('HOME'))) {
        return $settings;
    }

    $passFile = "{$homePath}/.pgpass";
    if (file_exists($passFile)) {
        $settings['passfile'] = $passFile;
    }

    $serviceFile = "{$homePath}/.pg_service.conf";
    if (file_exists($serviceFile)) {
        $settings['servicefile'] = $serviceFile;
    }

    $sslCert = "{$homePath}/.postgresql/postgresql.crt";
    $sslKey = "{$homePath}/.postgresql/postgresql.key";

    if (file_exists($sslCert) && file_exists($sslKey)) {
        // Both the cert and key must be present to use them, or do not use either
        $settings['sslcert'] = $sslCert;
        $settings['sslkey'] = $sslKey;
    }

    $sslRootCert = "{$homePath}\\.postgresql\\root.crt";
    if (file_exists($sslRootCert)) {
        $settings['sslrootcert'] = $sslRootCert;
    }

    return $settings;
}

/**
 * @return array<string, string>
 */
function getEnvSettings(): array
{
    /** @noinspection SpellCheckingInspection */
    static $nameMap = [
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

    $settings = [];

    foreach ($nameMap as $envName => $realName) {
        if (false !== ($value = getenv($envName))) {
            $settings[$realName] = $value;
        }
    }

    /** @var array<string, string> PHPStan */
    return $settings;
}

/**
 * @param string $serviceFilePath
 * @param string $serviceName
 * @return array<string, string>
 */
function getServiceSettings(string $serviceFilePath, string $serviceName): array
{
    $file = PgServiceFile::open($serviceFilePath);

    $service = $file->getServices()[$serviceName] ?? null;
    if ($service === null) {
        throw new InvalidArgumentException("Service {$serviceName} not found");
    }

    static $nameMap = [
        'dbname' => 'database',
    ];

    $settings = [];

    foreach ($service->settings as $key => $value) {
        $alias = $nameMap[$key] ?? $key;

        $settings[$alias] = $value;
    }

    /** @var array<string, string> PHPStan */
    return $settings;
}

/**
 * @param string $connString
 * @return array<string, string>
 *
 * @throws ConfigParseException
 */
function parseConnString(string $connString): array
{
    if ($connString === '') {
        return [];
    }

    if (
        str_starts_with($connString, 'postgres://') ||
        str_starts_with($connString, 'postgresql://')
    ) {
        return parseUrlSettings($connString);
    }

    return parseDSNSettings($connString);
}

/**
 * @param string $connString
 * @return array<string, string>
 *
 * @throws ConfigParseException
 */
function parseUrlSettings(string $connString): array
{
    $settings = [];

    $url = parse_url($connString);
    if ($url === false) {
        throw new ConfigParseException("Cannot parse connection string url: {$connString}");
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
                throw new ConfigParseException("Cannot split host with port on {$hostWithPort}", 0, $e);
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

        static $nameMap = [
            'dbname' => 'database',
        ];

        foreach ($queryVars as $key => $value) {
            $alias = $nameMap[$key] ?? $key;

            $settings[$alias] = $value;
        }
    }

    /** @var array<string, string> PHPStan */
    return $settings;
}

/**
 * @param string $connString
 * @return array<string, string>
 */
function parseDSNSettings(string $connString): array
{
    // TODO
    throw new InvalidArgumentException('DSN parsing is not implemented');
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
    return ip2long(trim($host, '[]')) !== false && !str_contains($host, ':');
}

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

/**
 * @param array<string, string> $settings
 * @param string $tlsHost
 * @return array{config: ClientTlsContext|null, sslMode: SslMode}
 */
function configTLS(array $settings, string $tlsHost): array
{
    $sslModeRaw = $settings['sslmode'] ?? 'prefer';
    $sslMode = SslMode::tryFrom($sslModeRaw);
    if ($sslMode === null) {
        throw new InvalidArgumentException("sslmode {$sslModeRaw} is invalid");
    }

    if ($sslMode === SslMode::DISABLE) {
        return ['config' => null, 'sslMode' => $sslMode];
    }

    $sslRootCert = $settings['sslrootcert'] ?? '';
    $sslCert = $settings['sslcert'] ?? '';
    $sslKey = $settings['sslkey'] ?? '';

    $config = new ClientTlsContext('');


    if ($sslMode === SslMode::ALLOW || $sslMode === SslMode::PREFER) {
        $config = $config->withoutPeerVerification();
    } elseif ($sslMode === SslMode::REQUIRE && $sslRootCert === '') {
        $config = $config->withoutPeerVerification();
    } elseif ($sslMode === SslMode::VERIFY_CA || ($sslMode === SslMode::REQUIRE && $sslRootCert !== '')) {
        // According to PostgreSQL documentation, if a root CA file exists,
        // the behavior of sslmode=require should be the same as that of verify-ca
        //
        // See https://www.postgresql.org/docs/12/libpq-ssl.html

        $config = $config
            ->withoutPeerVerification()
            ->withCaFile($sslRootCert);
    } elseif ($sslMode === SslMode::VERIFY_FULL) {
        $config = $config
            ->withPeerVerification()
            ->withPeerName($tlsHost)
            ->withCaFile($sslRootCert);
    } else {
        throw new \LogicException("Unknown state of sslmode {$sslModeRaw}");
    }

    if (($sslCert !== '' && $sslKey === '') || ($sslCert === '' && $sslKey !== '')) {
        throw new InvalidArgumentException("Both 'sslcert' and 'sslkey' are required");
    }

    if ($sslCert !== '' && $sslKey !== '') {
        $config = $config->withCertificate(new Certificate($sslCert, $sslKey));
    }

    return ['config' => $config, 'sslMode' => $sslMode];
}
