<?php

declare(strict_types=1);

namespace PhpPg\PgConn;

class CommandTag implements \Stringable
{
    public function __construct(private string $tag)
    {
    }

    /**
     * Returns the number of rows affected.
     * If the CommandTag was not for a row affecting command (e.g. "CREATE TABLE") then it returns 0.
     *
     * @return int
     */
    public function rowsAffected(): int
    {
        $spaceChar = \strrpos($this->tag, ' ');
        if (false === $spaceChar) {
            return 0;
        }

        return (int)\substr($this->tag, $spaceChar);
    }

    public function __toString(): string
    {
        return $this->tag;
    }
}
