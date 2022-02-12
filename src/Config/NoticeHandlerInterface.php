<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use PhpPg\PgConn\Notice;

interface NoticeHandlerInterface
{
    public function __invoke(Notice $notice): void;
}
