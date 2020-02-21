<?php
declare(strict_types=1);

namespace PDA;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;

use PDA\DynamoDbClientWrapper;

class PDA extends DynamoDbClientWrapper
{

    public function insert(array $columns, array $values): void
    {
        if (!count($columns) || !(count($columns) === count($values[0]))) {
            $this->throwMeBro();
        }

        $tableName = $this->getTableName();
        $params = [
            'TableName' => $tableName
        ];

        foreach ($values as $value) {
            $json = '{}';
            
            try {
                $json = json_encode(array_combine($columns, $value), JSON_THROW_ON_ERROR);
            } catch (JsonException $jsonException) {
                $this->throwMeBro($jsonException->getMessage());
            }

            $marshaler = new Marshaler();
            $params['Item'] = $marshaler->marshalJson($json);

            try {
                var_dump($this->getDynamoDbClient()->putItem($params));
             } catch (DynamoDbException $DynamoDbException) {
                $this->throwMeBro($DynamoDbException->getMessage());
            }
        }
    }

    public function select(array $columns = [], array $values = []): string
    {
        $tableName = $this->getTableName();
        $marshaler = new Marshaler();
        $aliases = [];
        $select = [];

        foreach($columns as $column) {
            if($this->isReservedKeyword($column)) {
                $aliases["#$column"] = $column;
                $select["#$column"] = $column;
                continue;
            }

            $select[$column] = $column;
        }

        $params = [
            'TableName' => $tableName,
            'ProjectionExpression' => implode(', ', array_keys($select)),
            'ExpressionAttributeNames' => $aliases
        ];

        try {
            $result = $this->getDynamoDbClient()->scan($params);
            $categories = [];

            foreach ($result['Items'] as $item) {
                $category = $marshaler->unmarshalItem($item);
                ksort($category);
                $categories[] = $category;
            }

            uasort($categories, static function ($a, $b) {
                return $a['name'] <=> $b['name'];
            });

            $categories = array_values($categories);

            sort($categories);

            return json_encode($categories, JSON_THROW_ON_ERROR, 512);
        } catch (DynamoDbException $dynamoDbException) {
            $this->throwMeBro($dynamoDbException->getMessage());
        }
    }
}
