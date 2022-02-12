<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config;

use PhpPg\PgConn\Notification;

interface NotificationHandlerInterface
{
    public function __invoke(Notification $notification): void;
}
