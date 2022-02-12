<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use function file_exists;
use function get_current_user;
use function getenv;

class DefaultSettingsParser
{
    /**
     * @return array<string, string>
     */
    public static function parse(): array
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $platformDependentOptions = static::getWindowsDefaultOptions();
        } else {
            $platformDependentOptions = static::getLinuxDefaultOptions();
        }

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
    public static function getWindowsDefaultOptions(): array
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
    public static function getLinuxDefaultOptions(): array
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

        $sslRootCert = "{$homePath}/.postgresql/root.crt";
        if (file_exists($sslRootCert)) {
            $settings['sslrootcert'] = $sslRootCert;
        }

        return $settings;
    }
}
