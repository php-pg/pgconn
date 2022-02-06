<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use PhpPg\PgServiceFile\PgServiceFile;

class ServiceFileParser
{
    private const NAME_MAP = [
        'dbname' => 'database',
    ];

    /**
     * @param string $serviceFilePath
     * @param string $serviceName
     * @return array<string, string>
     *
     * @throws \InvalidArgumentException
     */
    public static function parse(string $serviceFilePath, string $serviceName): array
    {
        $file = PgServiceFile::open($serviceFilePath);

        $service = $file->getServices()[$serviceName] ?? null;
        if ($service === null) {
            throw new \InvalidArgumentException("Service {$serviceName} not found");
        }

        $settings = [];

        foreach ($service->settings as $key => $value) {
            $settings[self::NAME_MAP[$key] ?? $key] = $value;
        }

        /** @var array<string, string> PHPStan */
        return $settings;
    }
}
