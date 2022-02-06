<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\Parser;

use InvalidArgumentException;

use function ltrim;
use function str_replace;
use function strlen;
use function strpos;
use function substr;
use function trim;

class DsnParser
{
    private const NAME_MAP = [
        'dbname' => 'database',
    ];

    private const ASCII_SPACE = [
        " " => true,
        "\t" => true,
        "\n" => true,
        "\r" => true,
    ];

    /**
     * @param string $connString
     * @return array<string, string>
     */
    public static function parse(string $connString): array
    {
        $settings = [];

        $remainingPart = $connString;
        while ($remainingPart !== '') {
            $eqPos = strpos($remainingPart, '=');
            if (false === $eqPos) {
                throw new InvalidArgumentException('Invalid DSN string');
            }

            $key = trim(substr($remainingPart, 0, $eqPos));
            $remainingPart = ltrim(substr($remainingPart, $eqPos + 1));

            if ($remainingPart === '') {
                break;
            }

            $remainingPartLen = strlen($remainingPart);
            $segmentEnd = 0;

            // Quoted param value parse
            if ($remainingPart[0] === "'") {
                $remainingPart = substr($remainingPart, 1);
                $remainingPartLen--;

                // Go until next quote character found
                for (; $segmentEnd < $remainingPartLen; $segmentEnd++) {
                    if ($remainingPart[$segmentEnd] === "'") {
                        break;
                    }

                    if ($remainingPart[$segmentEnd] === "\\") {
                        $segmentEnd++;
                    }
                }

                if ($segmentEnd === $remainingPartLen) {
                    throw new InvalidArgumentException('Unterminated quoted string in connection info string');
                }
            } else {
                // Go until space character found
                for (; $segmentEnd < $remainingPartLen; $segmentEnd++) {
                    if (self::ASCII_SPACE[$remainingPart[$segmentEnd]] ?? false) {
                        break;
                    }

                    if ($remainingPart[$segmentEnd] === "\\") {
                        $segmentEnd++;

                        if ($segmentEnd === $remainingPartLen) {
                            throw new InvalidArgumentException('Unexpected backslash found');
                        }
                    }
                }
            }

            // Unescape value
            $value = str_replace(
                ["\\\\", "\\'"],
                ["\\", "'"],
                substr($remainingPart, 0, $segmentEnd),
            );

            if ($segmentEnd === $remainingPartLen) {
                $remainingPart = '';
            } else {
                $remainingPart = substr($remainingPart, $segmentEnd + 1);
            }

            $settings[self::NAME_MAP[$key] ?? $key] = $value;
        }

        return $settings;
    }
}
