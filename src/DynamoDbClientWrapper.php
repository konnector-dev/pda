<?php

namespace PDA;

use Aws\Sdk;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;

use InvalidArgumentException;

abstract class DynamoDbClientWrapper {

    /**
     * @var DynamoDbClient
     */
    private DynamoDbClient $_dynamoDb;

    private $_tableName;

    protected array $reservedKeywords = [
        'name', 'status'
    ];

    protected function isReservedKeyword(string $keyword): bool
    {
        if(in_array($keyword, $this->reservedKeywords, false)) {
            return true;
        }

        return false;
    }

    protected function throwMeBro(string $message = 'Check for missing table name or mismatch of columns/values'): void
    {
        throw new InvalidArgumentException($message);
    }

    public function configureDynamoDbClient(array $configurations): DynamoDbClient 
    {
        try {
            $this->_dynamoDb = (new Sdk($configurations))->createDynamoDb();
         } catch (DynamoDbException $DynamoDbException) {
            $this->throwMeBro($DynamoDbException->getMessage());
        }

        return $this->getDynamoDbClient();
    }

    protected function getDynamoDbClient(): DynamoDbClient
    {
        if(empty($this->_dynamoDb)) {
            $this->throwMeBro('Please configure DynamoDb Client using configureDynamoDbClient(endpoint, region, version) method.');
        }

        return $this->_dynamoDb;
    }

    public function setTableName(string $tableName): DynamoDbClientWrapper
    {
        $this->_tableName = $tableName;

        return $this;
    }

    protected function getTableName(): string
    {
        if (empty($this->_tableName)) {
            $this->throwMeBro('Please set Table Name using setTableName(tableName) method.');
        }

        return $this->_tableName;
    }
    
}