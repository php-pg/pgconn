<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use InvalidArgumentException;
use PhpPg\PgConn\Config\Config;
use PhpPg\PgConn\Config\ConfigParseException;
use PhpPg\PgConn\Config\HostConfig;
use PhpPg\PgConn\Config\SslMode;
use PhpPg\PgConn\Config\TargetSessionAttrs;
use PhpPg\PgConn\Config\TlsConfig;
use PhpPg\PgConn\Config\ValidateConnect\ValidateConnectPrimary;
use PhpPg\PgConn\Config\ValidateConnect\ValidateConnectReadOnly;
use PhpPg\PgConn\Config\ValidateConnect\ValidateConnectReadWrite;
use PhpPg\PgConn\Config\ValidateConnect\ValidateConnectStandby;
use PhpPg\PgPassFile\PgPassFile;

use function array_key_exists;
use function array_merge;
use function explode;
use function is_numeric;
use function PhpPg\PgConn\Config\Internal\getNetworkAddress;
use function PhpPg\PgConn\Config\Internal\parsePort;

class ConfigParser
{
    private const NON_RUNTIME_PARAMS = [
        'host' => null,
        // not supported
        'hostaddr' => null,
        'port' => null,
        'database' => null,
        'user' => null,
        'password' => null,
        'passfile' => null,
        'connect_timeout' => null,
        // not supported
        'channel_binding' => null,
        // not supported
        'gssencmode' => null,
        // not supported
        'krbsrvname' => null,
        // not supported
        'gsslib' => null,
        // not supported (deprecated)
        'requiressl' => null,
        // not supported
        'requirepeer' => null,
        'sslmode' => null,
        'sslkey' => null,
        'sslcert' => null,
        'sslrootcert' => null,
        // not supported
        'sslpassword' => null,
        'sslsni' => null,
        // not supported
        'sslcrl' => null,
        // not supported (insecure)
        'sslcompression' => null,
        'ssl_min_protocol_version' => null,
        // not supported
        'ssl_max_protocol_version' => null,
        'target_session_attrs' => null,
        'service' => null,
        'servicefile' => null,
        // not supported
        'replication' => null,
        // ignored
        'tty' => null,
        // not supported
        'tcp_user_timeout' => null,
        // not supported
        'keepalives' => null,
        // not supported
        'keepalives_idle' => null,
        // not supported
        'keepalives_interval' => null,
        // not supported
        'keepalives_count' => null,
        // own custom params
        'min_read_buffer_size' => null,
        'ssl_security_level' => null,
        'ssl_verification_depth' => null,
        'ssl_ciphers' => null,
    ];

    /**
     * @param string $connString
     * @return Config
     *
     * @throws ConfigParseException
     */
    public static function parse(string $connString): Config
    {
        $defaultSettings = DefaultSettingsParser::parse();
        $envSettings = EnvParser::parse();

        try {
            $connStringSettings = ConnStringParser::parse($connString);
        } catch (InvalidArgumentException $e) {
            throw new ConfigParseException('Unable to parse connection string', 0, $e);
        }

        $settings = array_merge($defaultSettings, $envSettings, $connStringSettings);

        // Parse service file
        if (array_key_exists('service', $settings) && array_key_exists('servicefile', $settings)) {
            try {
                $serviceSettings = ServiceFileParser::parse($settings['servicefile'], $settings['service']);
            } catch (InvalidArgumentException $e) {
                throw new ConfigParseException('Unable to parse service file', 0, $e);
            }

            $settings = array_merge($defaultSettings, $envSettings, $serviceSettings, $connStringSettings);
        }

        $settings['user'] ??= '';
        $settings['database'] ??= '';
        $settings['password'] ??= '';

        $runtimeParameters = [];

        foreach ($settings as $key => $value) {
            if (array_key_exists($key, self::NON_RUNTIME_PARAMS)) {
                continue;
            }

            $runtimeParameters[$key] = $value;
        }

        // Parse Postgres password file
        $passFile = null;
        if (array_key_exists('passfile', $settings) && $settings['password'] === '') {
            try {
                $passFile = PgPassFile::open($settings['passfile']);
            } catch (InvalidArgumentException $e) {
                throw new ConfigParseException('Unable to parse password file', 0, $e);
            }
        }

        /** @var array<HostConfig> $hostConfigs */
        $hostConfigs = [];

        $hosts = explode(',', $settings['host']);
        $ports = explode(',', $settings['port']);

        foreach ($hosts as $idx => $host) {
            $portStr = $ports[$idx] ?? $ports[0];

            try {
                $port = parsePort($portStr);
            } catch (InvalidArgumentException $e) {
                throw new ConfigParseException("Invalid port {$portStr}", 0, $e);
            }

            ['network' => $network,] = getNetworkAddress($host, $port);

            /**
             * If a password file is used, you can have different passwords for different hosts.
             * All the other connection options are the same for every host in the list;
             * it is not possible to e.g., specify different usernames for different hosts.
             */
            $password = $settings['password']
                ?? $passFile?->findPassword($host, $portStr, $settings['database'], $settings['user'])
                ?? '';

            // Ignore TLS settings if Unix domain socket like libpq
            if ($network === 'unix') {
                $hostConfigs[] = new HostConfig(host: $host, port: $port, password: '', tlsConfig: null);
                continue;
            }

            try {
                $tlsConfig = self::configTLS($settings, $host);
            } catch (InvalidArgumentException $e) {
                throw new ConfigParseException('TLS Config error', 0, $e);
            }

            $hostConfigs[] = new HostConfig(
                host: $host,
                port: $port,
                password: $password,
                tlsConfig: $tlsConfig,
            );
        }

        if (array_key_exists('connect_timeout', $settings)) {
            if (!is_numeric($settings['connect_timeout'])) {
                throw new ConfigParseException('connect_timeout must be a valid float number');
            }

            $connectTimeout = (float)$settings['connect_timeout'];
            if ($connectTimeout < 0.001) {
                throw new ConfigParseException('connect_timeout be greater than 0.001 seconds');
            }
        }

        $targetSessionAttrs = TargetSessionAttrs::tryFrom($settings['target_session_attrs']);
        if ($targetSessionAttrs === null) {
            throw new ConfigParseException("target_session_attrs {$settings['target_session_attrs']} is invalid");
        }

        $validateConnect = match ($targetSessionAttrs) {
            TargetSessionAttrs::ANY => null,
            TargetSessionAttrs::READ_WRITE => new ValidateConnectReadWrite(),
            TargetSessionAttrs::READ_ONLY => new ValidateConnectReadOnly(),
            TargetSessionAttrs::PRIMARY => new ValidateConnectPrimary(),
            TargetSessionAttrs::STANDBY => new ValidateConnectStandby(),
            TargetSessionAttrs::PREFER_STANDBY => throw new ConfigParseException(
                'target_session_attrs prefer-standby is not supported'
            ),
        };

        return new Config(
            hosts: $hostConfigs,
            user: $settings['user'],
            database: $settings['database'],
            connectTimeout: $connectTimeout ?? 2.0,
            runtimeParams: $runtimeParameters,
            validateConnectFunc: $validateConnect,
        );
    }

    /**
     * @param array<string, string> $settings
     * @param string $tlsHost
     * @return TlsConfig|null
     *
     * @throws InvalidArgumentException
     */
    private static function configTLS(array $settings, string $tlsHost): ?TlsConfig
    {
        // Follow default libpq behavior and choose sslmode=prefer
        $sslModeRaw = $settings['sslmode'] ?? 'prefer';
        $sslMode = SslMode::tryFrom($sslModeRaw);
        if ($sslMode === null) {
            throw new InvalidArgumentException("sslmode {$sslModeRaw} is invalid");
        }

        if ($sslMode === SslMode::DISABLE) {
            return null;
        }

        $sslRootCert = $settings['sslrootcert'] ?? '';
        $sslCert = $settings['sslcert'] ?? '';
        $sslKey = $settings['sslkey'] ?? '';

        $tlsContext = (new ClientTlsContext(''))
            ->withSni()
            ->withMinimumVersion(ClientTlsContext::TLSv1_2)
            ->withoutPeerVerification();

        if (($settings['sslsni'] ?? '') === '0') {
            $tlsContext = $tlsContext->withoutSni();
        }

        if (array_key_exists('ssl_security_level', $settings)) {
            $securityLevel = $settings['ssl_security_level'];
            $sslSecurityLevel = match ($securityLevel) {
                '0', '1', '2', '3', '4', '5' => (int)$securityLevel,
                default => throw new InvalidArgumentException("ssl_security_level {$securityLevel} is invalid")
            };

            $tlsContext = $tlsContext->withSecurityLevel($sslSecurityLevel);
        }

        if (array_key_exists('ssl_verification_depth', $settings)) {
            $verificationDepth = $settings['ssl_verification_depth'];
            if (!\ctype_digit($verificationDepth)) {
                throw new InvalidArgumentException("ssl_verification_depth {$verificationDepth} is invalid");
            }

            $tlsContext = $tlsContext->withVerificationDepth((int)$verificationDepth);
        }

        if (array_key_exists('ssl_ciphers', $settings)) {
            $tlsContext = $tlsContext->withCiphers($settings['ssl_ciphers']);
        }

        if (array_key_exists('ssl_min_protocol_version', $settings)) {
            $minProtocol = $settings['ssl_min_protocol_version'];

            $sslMinProtocol = match ($minProtocol) {
                'TLSv1', 'TLSv1.0' => ClientTlsContext::TLSv1_0,
                'TLSv1.1' => ClientTlsContext::TLSv1_1,
                'TLSv1.2' => ClientTlsContext::TLSv1_2,
                'TLSv1.3' => ClientTlsContext::TLSv1_3,
                default => throw new InvalidArgumentException("Invalid ssl_min_protocol_version {$minProtocol}"),
            };
            $tlsContext = $tlsContext->withMinimumVersion($sslMinProtocol);
        }

        // ssl_max_protocol_version is not supported due to lack of ClientTlsContext functionality
        // sslpassword is not supported due to lack of ClientTlsContext functionality

        if ($sslMode === SslMode::ALLOW || $sslMode === SslMode::PREFER) {
            // OK: Nothing to do
        } elseif ($sslMode === SslMode::REQUIRE && $sslRootCert === '') {
            // OK: Nothing to do
        } elseif ($sslMode === SslMode::VERIFY_CA || ($sslMode === SslMode::REQUIRE && $sslRootCert !== '')) {
            // According to PostgreSQL documentation, if a root CA file exists,
            // the behavior of sslmode=require should be the same as that of verify-ca
            //
            // See https://www.postgresql.org/docs/current/libpq-ssl.html

            $tlsContext = $tlsContext
                ->withoutPeerVerification()
                ->withCaFile($sslRootCert);
        } elseif ($sslMode === SslMode::VERIFY_FULL) {
            $tlsContext = $tlsContext
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
            $tlsContext = $tlsContext->withCertificate(new Certificate($sslCert, $sslKey));
        }

        return new TlsConfig(tlsContext: $tlsContext, sslMode: $sslMode);
    }
}
