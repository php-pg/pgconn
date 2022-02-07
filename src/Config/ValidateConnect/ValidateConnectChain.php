<?php

declare(strict_types=1);

namespace PhpPg\PgConn\Config\ValidateConnect;

use PhpPg\PgConn\Config\ValidateConnectFuncInterface;
use PhpPg\PgConn\PgConn;

class ValidateConnectChain implements ValidateConnectFuncInterface
{
    /**
     * @param array<ValidateConnectFuncInterface> $chain
     */
    public function __construct(
        private array $chain
    ) {
    }

    public function __invoke(PgConn $conn): void
    {
        foreach ($this->chain as $item) {
            $item($conn);
        }
    }
}
