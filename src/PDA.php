<?php
declare(strict_types=1);

namespace PDA;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use InvalidArgumentException;
use JsonException;

class PDA
{
    /**
     * @var DynamoDbClient
     */
    private DynamoDbClient $_dynamoDb;
    private array $_reservedKeywords = [
        'name', 'status', 'items'
    ];
    private array $_key;
    private string $_expressionString = '';
    private array $_expressionAttributeValuesArray = [];
    private array $_expressionAttributeNames = [];

    public function __construct(DynamoDbClient $dynamoDb)
    {
        $this->_dynamoDb = $dynamoDb;
    }

    private function _isReservedKeyword(string $keyword): bool
    {
        if (in_array($keyword, $this->_reservedKeywords, false)) {
            return true;
        }
        return false;
    }

    private function throwMeBro(string $message = 'Check for missing table name or mismatch of columns/values'): void
    {
        throw new InvalidArgumentException($message);
    }

    public function insert(string $table, array $columns, array $values): void
    {
        if ($table === '' || !count($columns) || !(count($columns) === count($values[0]))) {
            $this->throwMeBro();
        }

        $params = [
            'TableName' => $table
        ];

        foreach ($values as $value) {
            $json = '{}';

            try {
                $json 
                    = json_encode(array_combine($columns, $value), JSON_THROW_ON_ERROR);
            } catch (JsonException $jsonException) {
                $this->throwMeBro($jsonException->getMessage());
            }

            $marshaler = new Marshaler();
            $params['Item'] = $marshaler->marshalJson($json);

            try {
                $this->_dynamoDb->putItem($params);
            } catch (DynamoDbException $DynamoDbException) {
                $this->throwMeBro($DynamoDbException->getMessage());
            }
        }
    }

    public function select(string $table, array $columns = []): string
    {
        if ($table === '') {
            $this->throwMeBro();
        }

        $marshaler = new Marshaler();

        $aliases = [];
        $select = [];
        
        foreach ($columns as $column) {
            if ($this->_isReservedKeyword($column)) {
                $aliases["#$column"] = $column;
                $select["#$column"] = $column;
                continue;
            }

            $select[$column] = $column;
        }

        $params = [
            'TableName' => $table,
            'ProjectionExpression' => implode(', ', array_keys($select)),
            'ExpressionAttributeNames' => $aliases
        ];

        $responseArray = [];
        try {
            $result = $this->_dynamoDb->scan($params);

            foreach ($result['Items'] as $item) {
                $responseArrayItem = $marshaler->unmarshalItem($item);
                ksort($responseArrayItem);
                $responseArray[] = $responseArrayItem;
            }

            uasort(
                $responseArray, 
                static function ($a, $b) {
                    return $a['name'] <=> $b['name'];  
                }
            );

            $responseArray = array_values($responseArray);

            sort($responseArray);

        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }

        return json_encode($responseArray);
    }

    public function update(string $table): string
    {
        if ($table === '') {
            $this->throwMeBro();
        }

        $updateExpression = $this->_getUpdateExpressionString();

        $params = [
            'TableName' => $table,
            'Key' => $this->_getKey(),
            'UpdateExpression' => $updateExpression,
            'ExpressionAttributeValues'=> $this->_getExpressionAttributeValues(),
            'ReturnValues' => 'UPDATED_NEW'
        ];

        if (!empty($this->_expressionAttributeNames)) {
            $params['ExpressionAttributeNames'] = $this->_expressionAttributeNames;
        }

        $response = '';
        try {
            $response = $this->_dynamoDb->updateItem($params);
        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }
        return json_encode($response);
    }

    public function key(array $keyArray): PDA
    {
        $marshaler = new Marshaler();
        $keyJson = '{}';

        try {
            $keyJson = json_encode($keyArray, JSON_THROW_ON_ERROR);
        } catch (JsonException $jsonException) {
            $this->throwMeBro($jsonException->getMessage());
        }

        $this->_key = $marshaler->marshalJson($keyJson);

        return $this;
    }

    private function _getKey(): array
    {
        return $this->_key;
    }

    private function _getExpressionAttributeValues(): array
    {
        return $this->_expressionAttributeValuesArray;
    }

    private function _getUpdateExpressionString(): string
    {
        return $this->_expressionString;
    }

    private function _setExpressionAttributeValueItem(string $key, $value = null): void
    {
        $item = [];
        $item[$this->_renameReservedKeywords($key, true)] = $value;
        $marshaler = new Marshaler();
        $itemJson = '{}';

        try {
            $itemJson = json_encode($item, JSON_THROW_ON_ERROR);
        } catch (JsonException $jsonException) {
            $this->throwMeBro($jsonException->getMessage());
        }

        $marshaledItem = $marshaler->marshalJson($itemJson);

        $this->_expressionAttributeValuesArray 
            = array_merge($this->_expressionAttributeValuesArray, $marshaledItem);
    }

    public function set(array $dataArray): PDA
    {
        $this->_expressionString .= (!empty($this->_expressionString))? ' ': '';
        $this->_expressionString .= 'set ';
        $iterationCount = 0;

        foreach ($dataArray as $key => $value) {
            $this->_setExpressionAttributeValueItem($key, $value);

            $this->_expressionString .= ($iterationCount > 0) ? ', ' : '';
            $this->_expressionString .= $this->_renameReservedKeywords($key);
            $this->_expressionString .= ' = ' . $this->_renameReservedKeywords(
                $key, 
                true
            );

            $iterationCount++;
        }

        return $this;
    }

    private function _renameReservedKeywords(string $column, $isExpressionAttributeValue = false): string
    {
        $alias = '';

        if ($this->_isReservedKeyword($column) && !$isExpressionAttributeValue) {
            $alias = '#' . $column;
            $this->_expressionAttributeNames[$alias] = $column;
        } elseif ($isExpressionAttributeValue) {
            $alias = ':' . $column;
        }

        return (empty($alias))? $column : $alias;
    }

    public function delete(string $table): string
    {
        if ($table === '') {
            $this->throwMeBro();
        }

        $params = [
            'TableName' => $table,
            'Key' => $this->_getKey()
        ];

        $response = '';
        try {
            $response = $this->_dynamoDb->deleteItem($params);
        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }
        return json_encode($response);
    }
}
