# pgconn

Package pgconn is a low-level PostgreSQL database driver. It operates at nearly the same level as the C library libpq.
Applications should handle normal queries with a higher level library and only use pgconn directly when needed for
low-level access to PostgreSQL functionality.

## Connecting
This library follows `libpq` behavior as much as possible.
This means that you can use almost all `libpq` connection options and both connection string formats.

Supported `libpq` features:
* [.pgpass](https://www.postgresql.org/docs/current/libpq-pgpass.html) file
* [.pg_service.conf](https://www.postgresql.org/docs/current/libpq-pgservice.html) file
* [Environment Variables](https://www.postgresql.org/docs/current/libpq-envars.html)
* Multiple connection hosts

### Using connection string
Read about `libpq` connection string format [34.1.1. Connection Strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

Supported connection strings:
* Keyword/Value, example: `host=localhost port=5432 user=user password=secret dbname=mydb connect_timeout=10`
  * Multi-hosts, example: `host=localhost,otherhost port=5432,5433 user=user password=secret dbname=mydb connect_timeout=10`
* URI, example: `postgresql://user:secret@localhost:5432/mydb?connect_timeout=10`
  * Multi-hosts, example: `postgresql://user:secret@localhost:5432,otherhost:5433/mydb?connect_timeout=10`
```php
$connString = 'host=localhost port=5432 user=user password=secret dbname=mydb connect_timeout=10';
// or
$connString = 'postgresql://user:secret@localhost:5432/mydb?connect_timeout=10';

$conf = \PhpPg\PgConn\Config\Internal\parseConfig($connString);

$pgConnector = new \PhpPg\PgConn\PgConnector();
$conn = $pgConnector->connect($conf);
```

Read about supported connection options [34.1.2. Parameter Key Words](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)

### By instantiating config manually

```php
$conf = new \PhpPg\PgConn\Config\Config(
    hosts: [
        new \PhpPg\PgConn\Config\HostConfig(
            host: '127.0.0.1',
            port: 5432,
            password: 'secret',
            tlsConfig: null,
        ),
        new \PhpPg\PgConn\Config\HostConfig(
            host: '127.0.0.1',
            port: 5433,
            password: 'secret_pass',
            tlsConfig: new \PhpPg\PgConn\Config\TlsConfig(
                tlsContext: (new \Amp\Socket\ClientTlsContext(''))
                    ->withoutPeerVerification()
                    ->withCaFile('path to CA')
                    ->withCertificate(new \Amp\Socket\Certificate('path to public key', 'path to private key')),
                sslMode: \PhpPg\PgConn\Config\SslMode::PREFER,
            )
        ),
    ],
    user: 'postgres',
    database: 'mydb',
    connectTimeout: 1,
    // and other params
);

// Also, fluent interface supported
$conf = $conf
    ->withRuntimeParam('application_name', 'My Application')
    ->withOnNotice(new class implements \PhpPg\PgConn\Config\NoticeHandlerInterface {
        public function __invoke(\PhpPg\PgConn\Notice $notice): void {
            // do something with notice
        }
    })
    ->withOnNotification(new class implements \PhpPg\PgConn\Config\NotificationHandlerInterface {
        public function __invoke(\PhpPg\PgConn\Notification $notice): void {
            // do something with notification
        }
    })
    // Any PSR compatible logger for debugging purposes
    ->withLogger($logger);

$pgConnector = new \PhpPg\PgConn\PgConnector();
$conn = $pgConnector->connect($conf);
```

## API
PgConn is not concurrency safe, it should be used inside a connection pool.

Methods:
* `PgConn::close(): void` - Disconnect from the server
* `PgConn::getConfig(): Config` - Get config that was used to establish the connection
* `PgConn::getHostConfig(): HostConfig` - Get config for the currently connected host
* `PgConn::getSocket(): Socket` - Get underlying socket connection
* `PgConn::getStatus(): PgConnStatus` - Get current connection state
* `PgConn::getPid(): int` - Get backend process id
* `PgConn::getSecretKey(): int` - Get secret key to cancel requests
* `PgConn::getTxStatus(): string (1 byte)` - Get transaction status (see ReadyForQuery message for details)
* `PgConn::getParameterStatuses(): array<string, string>` - Get current session runtime parameters
* `PgConn::getParameterStatus(string $paramName): string` - Get current session runtime parameter
* `PgConn::cancelRequest` - Cancel running in-progress request (might not have effect, see `Cancelling API calls` section)
* `PgConn::exec` - Execute query using simple protocol (multiple queries can be specified at once).
  <details>
    <summary>See example</summary>
  
  ```php
  /** @var \PhpPg\PgConn\PgConn $conn */
  $sql = "SELECT 'Hello World' AS msg, 322 AS num; SELECT * FROM table; UPDATE table SET idx = idx + 1";
  
  /**
  * If the query contains a syntax error or contains invalid data, no exception will be thrown,
  * an exception will be thrown when the results are received.
  */
  $mrr = $conn->exec($sql);
  
  // Fetch all results at once
  $results = $mrr->readAll();
  
  $results[0]; // query 1 results
  $results[1]; // query 2 results
  $results[2]; // query 3 results
  
  $results[0]->getCommandTag(); // query 1 execution result (e.g. SELECT 1)
  $rows = $results[0]->getRows(); // query 1 returned rows (multidimensional array)
  $rows[0]; // query 1 row 1
  $rows[0][0] // query 1 row 1 column 1
  $rows[0][0] // query 1 row 1 column 2
  
  $results[0]->getFieldDescriptions(); // query 1 returned rows format information (binary/text, data type, column name, etc)
  
  // Fetch results in iterative way
  while ($mrr->nextResult()) {
      $rr = $mrr->getResultReader();
      $fieldDescriptions = $rr->getFieldDescriptions()
      
      while ($rr->nextRow()) {
          $result = $rr->getResult();
  
          $rows = $result->getRows();
          foreach ($rows as $rowIdx => $row) {
              foreach ($row as $colIdx => $colValue) {
                  // Do something with returned data
              }
          }
      }
      
      $commandTag = $rr->getCommandTag();
  }
  ```
  </details>

* `PgConn::execParams` - Execute query using extended protocol (parameter bindings allowed), multiple queries are not allowed
  <details>
    <summary>See example</summary>
  
  ```php
  /** @var \PhpPg\PgConn\PgConn $conn */
  $rr = $conn->execParams(
      sql: 'SELECT $1::int, $2::text',
      paramValues: ['100', 'Hello world'],
      // param formats (binary/text)
      paramFormats: [],
      // param data types
      paramOIDs: [],
      // return rows format (binary/text)
      resultFormats: [],
  );
  
  $result = $rr->getResult();
  
  $result->getFieldDescriptions(); // returned rows format information (binary/text, data type, column name, etc)
  $result->getRows(); // returned rows
  $result->getCommandTag(); // command execution result
  ```
  </details>

* `PgConn::prepare` - Prepare statement
  <details>
    <summary>See example</summary>
  
    ```php
    /** @var \PhpPg\PgConn\PgConn $conn */
    $stmtDesc = $conn->prepare(
        name: 'my_stmt_1',
        sql: "INSERT INTO my_table (col1, col2) VALUES ($1::int, $2::text)"
    );
    $stmtDesc->name; // prepared statement name
    $stmtDesc->fields; // prepared statement return rows format description
    $stmtDesc->paramOIDs; // prepared statement bind parameter types
    $stmtDesc->sql; // prepared statement query
    ```
  </details>

* `PgConn:execPrepared` - Execute prepared statement
  <details>
    <summary>See example</summary>
  
  ```php
  /** @var \PhpPg\PgConn\PgConn $conn */
  $rr = $conn->execPrepared(
      stmtName: 'my_stmt_1',
      paramValues: ['100', 'Hello World'],
      // parameter formats (1 - text; 0 - binary
      // One item per each paramValue or one item for all paramValues
      paramFormats: [],
      // desired format of returned rows, such as paramFormats
      resultFormats: [],
  );
  $result = $rr->getResult();
  
  $result->getFieldDescriptions(); // returned rows format information (binary/text, data type, column name, etc)
  $result->getRows(); // returned rows
  $result->getCommandTag(); // command execution result
  ```
  </details>

* `PgConn::copyFrom` - Copy data from readable stream to the PostgreSQL server
  <details>
    <summary>See example</summary>
  
  ```php
  /** @var \PhpPg\PgConn\PgConn $conn */
  $genData = static function (): \Generator {
      for ($i = 0; $i < 1000; $i++) {
          yield "{$i}, \"foo {$i} bar\"\n";
      }
  };
  $stream = new \Amp\ByteStream\ReadableIterableStream($genData());
  
  $ct = $conn->copyFrom('COPY foo FROM STDIN WITH (FORMAT csv)', $stream);
  echo "Rows affected: {$ct->rowsAffected()}\n";
  ```
  </details>

* `PgConn::copyTo` - Copy data from the PostgreSQL server to writable stream
  <details>
    <summary>See example</summary>
  
  ```php
  /** @var \PhpPg\PgConn\PgConn $conn */
  $stream = new \Amp\ByteStream\WritableIterableStream(0);
  
  \Amp\async(static function () use ($stream) {
  foreach ($stream->getIterator() as $row) {
  // Process row copied from postgres
  }
  });
  
  $ct = $conn->copyTo('COPY foo TO STDOUT', $stream);
  echo "Rows affected: {$ct->rowsAffected()}\n";
  ```
  </details>

## Cancelling API calls
Any of API calls can be canceled using AMPHP cancellation objects. \
When cancellation occurs, library sends `CancelRequest` message to the PostgreSQL server.

But there is an **important note** from the PostgreSQL Protocol Flow:

The cancellation signal might or **might not** have any effect — for example, 
if it arrives after the backend has finished processing the query, then it will have no effect. \
If the cancellation is effective, it results in the current command being terminated early with an error message.

The upshot of all this is that for reasons of both security and efficiency, 
the frontend has no direct way to tell whether a cancel request has succeeded. \
It **must continue to wait** for the backend to respond to the query. \
Issuing a cancel simply improves the odds that the current query will finish soon, 
and improves the odds that it will fail with an error message instead of succeeding.

This means that a Cancellation object **may not** immediately cancel a running query.

See for more information [53.2.7. Canceling Requests in Progress](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.9)


##

Inspired by Golang package [jackc/pgconn](https://github.com/jackc/pgconn)
