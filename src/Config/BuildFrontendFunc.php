<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use Amp\ByteStream\WritableStream;
use PhpPg\PgProto3\ChunkReaderInterface;
use PhpPg\PgProto3\FrontendInterface;
use Psr\Log\LoggerInterface;

interface BuildFrontendFunc
{
    public function __invoke(
        ChunkReaderInterface $reader,
        WritableStream $writer,
        ?LoggerInterface $logger
    ): FrontendInterface;
}
