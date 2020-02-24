<?php

namespace PDA;

use \RecursiveIteratorIterator;
use \RecursiveArrayIterator;

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
    private $_key;

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
            $this->throwMeBro('Please configure DynamoDb Client using configureDynamoDbClient(["endpoint" => "http://localhost:8000", "region" => "ap-south-1", "version" => "latest"
            ]) method.');
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

    public function setKey(array $key = []): PDA
    {
        $this->_key = $key;

        return $this;
    }

    protected function getKey(): array
    {
        return $this->_key;
    }

    protected function prepareUpdateExpressionString(array $data): string
    {

        $updateExpressionString = 'set ';
        $iterationCount = 0;
        $recursiveIteratorIterator = new RecursiveIteratorIterator(new RecursiveArrayIterator($data));

        foreach ($recursiveIteratorIterator as $elementValue) {

            $keys = [];

            foreach (range(0, $recursiveIteratorIterator->getDepth()) as $depth) {
                $keys[] = $recursiveIteratorIterator->getSubIterator($depth)->key();
            }

            $updateExpressionString .= ($iterationCount > 0) ? ', ' : '';
            $updateExpressionString .= (sizeof($keys) > 1)? join('.', $keys) : $this->_renameReservedKeywords(join('.', $keys));
            $updateExpressionString .= ' = :' . $keys[(sizeof($keys) -1)];

            $iterationCount++;
        }

        return $updateExpressionString;
    }

    private function _renameReservedKeywords(string $column, $isExpressionAttributeValue = false): string
    {
        $alias = '';

        if ($this->isReservedKeyword($column) && !$isExpressionAttributeValue) {
            $alias = '#' . $column;
            $this->_expressionAttributeNames[$alias] = $column;
        } elseif ($isExpressionAttributeValue) {
            $alias = ':' . $column;
        }

        return (empty($alias))? $column : $alias;
    }

    protected function prepareExpressionAttributeValuesArray(array $dataArray): array
    {
        $expressionAttributeValuesArray = [];

        foreach ($dataArray as $key => $value) {
            $expressionAttributeValuesArray[$this->_renameReservedKeywords($key, true)] = $value;
        }

        return $expressionAttributeValuesArray;
    }
    
}