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
        foreach ($this->chain as $idx => $item) {
            if (!$item instanceof ValidateConnectFuncInterface) {
                throw new \InvalidArgumentException(
                    "Item at {$idx} must be an instance of ValidateConnectFuncInterface"
                );
            }
        }
    }

    public function __invoke(PgConn $conn): void
    {
        foreach ($this->chain as $item) {
            $item($conn);
        }
    }
}
