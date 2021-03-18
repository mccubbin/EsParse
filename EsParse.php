<?php

namespace App\Library;

use Illuminate\Support\Facades\DB;


class EsParse {

    public static function searchMatch($viewTable, $query) {
        $qData['index'] = $viewTable;
        if (isset($query['match'])) {
            $query['must'] = $query['match'];
            unset($query['match']);
        }
        $qData['query'] = $query;

        $data = self::searchQuery($qData);

        return $data;
    }

    public static function searchQuery($qData) {
        list($data) = self::searchQueryTotal($qData);
        return $data;
    }

    public static function searchQueryTotal($qData, $vData = []) {
        // 1 get query parts
        $viewTable      = isset($qData['index']) ? $qData['index'] : null;
        $query          = isset($qData['query']) ? $qData['query'] : null;
        $size           = isset($qData['size']) ? $qData['size'] : null;
        $from           = isset($qData['from']) ? $qData['from'] : null;
        $sort           = isset($qData['sort']) ? $qData['sort'] : [];
        $source         = isset($qData['_source']) ? $qData['_source'] : null;
        $isFirstRowOnly = isset($qData['isFirstRowOnly']) ? $qData['isFirstRowOnly'] : false;


        // 2 get column list
        $predicates = self::parseEsArrayPredicates($query, $vData);


        // 3 categorize column types: DB or ES
        list($db_preds, $es_preds) = self::groupPredicates($viewTable, $predicates);

        // 4 build and run db query
        $dbResults = self::runDbPredicates($viewTable, $db_preds);


        // 5 get raw ES results
        $elasticClass  = '\App\Console\InsertToElastic\ViewTableClass\\' . $viewTable;
        $esData = $elasticClass::parseData(null, $dbResults);


        // 6 filter collection with ES predicates
        $esData = collect($esData);
        $esData = self::runEsPredicates($esData, $es_preds);


        // 7 get count
        $total = $esData->count();


        // 8 order by fields $sort
        $esData = self::sortEsCollection($esData, $sort);


        // 9 filter specific fields
        $esData = self::filterCollectionFields($esData, $source);


        // 10 if first row only, return now
        if ($isFirstRowOnly || $size === 1) {
            $esData = $esData->first() ?: [];
            return [$esData, $total];
        }


        // 11 size limit and offset
        $esData = $esData->slice($from, $size);


        // 12 reNumber keys from 0
        $esData = self::reIndexCollection($esData);


        // 13 convert collection to array
        $esData = $esData->toArray();


        return [$esData, $total];
    }



    #####################################################################################
    # parse ES array methods ############################################################
    #####################################################################################

    public static function parseEsArrayPredicates($where, $filters = []) {
        $columns = [];

        // If url param or search form filters are present, refactor ES query structure
        if ((isset($filters['defaultFilter']) && !empty($filters['defaultFilter'])) || isset($filters['filter'])) {
            $mustFilters = self::handleFilters(
                $where['raw'],
                'must',
                (isset($filters['defaultFilter']) && !empty($filters['defaultFilter']))
            );
            $where['must'] = isset($where['must'])
                ? array_merge($where['must'], $mustFilters)
                : $mustFilters;
        }

        // if 1 layer of raw element in array, merge its contents in & remove it
        if ( isset($where['raw']) && is_array($where['raw']) ) {
            $where = array_merge($where, $where["raw"]);
            unset($where['raw']);
        }

        // "must" to ->where()
        if ( isset($where["must"]) ) {

            $where['must'] = self::cleanPredicateArray($where['must']);

            foreach ($where["must"] as $field => $value ) {

                // ignore null, false, empty strings, empty arrays; but not 0, '0', arrays
                if ( empty($value) && $value !== '0' && $value !== 0 ) {
                    continue;
                }

                // if where clause is stored in numeric arrays, extract values
                if ( is_int($field) && is_array($value) && count($value) == 1 && !isset($value["bool"]) ) {
                    $field = key($value);
                    $value = current($value);
                }

                // if "term" or "terms" exists, extract values
                if ( $field !== 0 && in_array($field, ["term", "terms"] ) && is_array($value) && count($value) == 1 ) {
                    $field = key($value);
                    $value = current($value);
                }

                $field = str_replace(".keyword", "", $field);

                if (is_array($value)) {

                    // if this is a double query_string search, toss out first array
                    if ( isset($value['bool']) && count($value['bool']) == 1 && isset($value['bool']['should'][0]['query_string']) ) {
                        $field = 'search';
                        $value = $value['bool']['should'][1]['term'];
                    } elseif ( $field == 'bool' && count($value) == 1 && isset($value['should'][0]['query_string']) ) {
                        $field = 'search';
                        $value = $value['should'][1]['term'];
                    }

                    if ($field === 'wildcard'){
                        foreach($value as $column => $wildcardValue){
                            $column = str_replace(".keyword", "", $column);
                            $columns[$column][] = ['val' => $wildcardValue, 'op' => 'like'];
                        }
                    } elseif ($field === 'search') {
                        foreach ($value as $column => $value) {
                            $column = str_replace(".keyword", "", $column);
                            $value = $value;
                            $columns[$column][] = ['val' => $value, 'op' => 'like'];
                        }
                    } elseif (isset($value['term']) && is_array($value['term']) ) {
                        foreach ($value['term'] as $tcolumn => $tvalue) {
                            $tcolumn = str_replace(".keyword", "", $tcolumn);
                            $columns[$tcolumn][] = ['val' => $tvalue, 'op' => 'equal'];
                        }
                    } elseif ($field == "range" && is_array($value) ) {
                        $columns = array_merge($columns, self::getQueryRangeWhere($value));

                    } elseif ( isset($value["range"]) && is_array($value["range"]) ) {
                        $columns = array_merge($columns, self::getQueryRangeWhere($value["range"]));
                    } else {
                        $columns[$field][] = ['val' => $value, 'op' => 'in'];
                    }
                } else {
                    $value = self::convertIfDate($value);

                    if (str_contains($value, '%')) {
                        $columns[$field][] = ['val' => $value, 'op' => 'like'];
                    } else {
                        $columns[$field][] = ['val' => $value, 'op' => 'equal'];
                    }
                }
            }
        }

        // "must_not" to ->where()
        if ( isset($where['must_not']) ) {

            $where['must_not'] = self::cleanPredicateArray($where['must_not']);

            foreach ($where['must_not'] as $field => $value ) {

                // ignore null, false, empty strings, empty arrays; but not 0, '0', arrays
                if ( empty($value) && $value !== '0' && $value !== 0 ) {
                    continue;
                }

                // if where clause is stored in numeric arrays, extract values
                if ( is_int($field) && is_array($value) && count($value) == 1 ) {
                    $field = key($value);
                    $value = current($value);
                }

                // if "term" or "terms" exists, extract values
                if ( $field !== 0 && in_array($field, ["term", "terms"] ) && is_array($value) && count($value) == 1 ) {
                    $field = key($value);
                    $value = current($value);
                }

                $field = str_replace(".keyword", "", $field);
                if (is_array($value)) {
                    if($field === 'wildcard'){
                        foreach ($value as $column => $wildcardValue ) {
                            $column = str_replace(".keyword", "", $column);
                            $columns[$column][] = ['val' => $wildcardValue, 'op' => 'not_like'];
                        }
                    } else {
                        $columns[$field][] = ['val' => $value, 'op' => 'not_in'];
                    }
                } else {
                    $value = self::convertIfDate($value);

                    if (str_contains($value, '%')) {
                        $columns[$field][] = ['val' => $value, 'op' => 'not_like'];
                    } else {
                        $columns[$field][] = ['val' => $value, 'op' => 'not_equal'];
                    }
                }
            }
        }

        return $columns;
    }

    public static function cleanPredicateArray($array, $like=false) {

        foreach ($array as &$value) {
            // eliminate ["bool"]["should"] from array
            if ( isset($value["bool"]) && isset($value["bool"]["should"]) ) {

                // if "query_string" is present, the accompanying array is a like
                if ( isset($value["bool"]["should"][0]) && isset($value["bool"]["should"][0]["query_string"]) ) {
                    unset($value["bool"]["should"][0]);
                    $slice = self::cleanPredicateArray($value["bool"]["should"], true);
                } else {
                    $slice = self::cleanPredicateArray($value["bool"]["should"]);
                }
                unset($value["bool"]["should"], $value["bool"]);
                $array = array_merge($array, $slice);
            } elseif ( isset($value["terms"]) ) {
                $value = $value["terms"];
            } elseif ( isset($value["term"]) ) {
                $value = $value["term"];
                if ($like) {
                    foreach ($value as &$val) {
                        $val = '%' . $val . '%';
                    }
                }
            }
        }

        // remove null, false, empty strings, empty arrays; leave 0, '0', arrays
        $array = array_filter($array, function($value) {
            return !(empty($value) && $value !== 0 && $value !== '0');
        });

        return $array;
    }

    public static function getQueryRangeWhere($range) {
        $rangeOperators = [
            'gt' => '>',
            'lt' => '<',
            'gte' => '>=',
            'lte' => '<=',
        ];
        foreach ($range as $range_column => $range_vals) {
            $range_column = str_replace(".keyword", "", $range_column);

            foreach($range_vals as $range_operator => $range_val){
                $columns[$range_column][] = ['val' => $range_val, 'op' => $range_operator];
            }
        }
        return $columns;
    }

    /*
     * Refactor the ES query structure for handling the url/search form parameters
     * for filtering the data lists (ex: /tenant#prop=123456&unit=2121&tenant=2)
     * */
    public static function handleFilters($filters, $must = 'must', $strictMatch = true) {
        foreach($filters[$must] as $key => $value){
            if(is_numeric($key)){
                if(isset($filters[$must][$key]['bool']['should'][1]['term'])){
                    $wildCard = !$strictMatch ? '%' : '';
                    $filters[$must][key($filters[$must][$key]['bool']['should'][1]['term'])] = $wildCard . array_values($filters[$must][$key]['bool']['should'][1]['term'])[0] . $wildCard;
                    unset($filters[$must][$key]);
                }
            }
        }
        return $filters[$must];
    }

    public static function convertIfDate($value) {
        if ( ($date = \DateTime::createFromFormat('m/d/Y', trim($value, '%'))) !== false ) {
            $value = $date->format('Y-m-d');
        }
        return $value;
    }



    #####################################################################################
    # misc collection methods ###########################################################
    #####################################################################################

    public static function reIndexCollection($collection) {
        // reset array keys from 0
        $collection = $collection->values();

        return $collection;
    }

    public static function filterCollectionFields($collection, $fields) {

        if ( !empty($fields) && (isset($fields[0]) && $fields[0] !== '*') ) {
            $collection = $collection->map(function ($row) use ($fields) {
                return collect($row)->only($fields)->toArray();
            });
        }

        return $collection;
    }

    public static function sortEsCollection($esData, $sort) {
        $sort = array_reverse($sort);
        $array = $esData->toArray();

        foreach ($sort as $field => $direction) {
            $field = str_replace(".keyword", "", $field);
            if ('desc' ===  strtolower($direction)) {
                $array = self::maSort($array, $field, 2);
            } else {
                $array = self::maSort($array, $field, 1);
            }
        }
        $esData = collect($array);

        return $esData;
    }

    /**
     *  sorts multiarray by a subarray value while preserving all keys,
     *  also preserves original order when the sorting values match
     */
    public static function maSort($ma, $sortkey, $sortorder = 1) {
        // confirm inputs
        if ($ma && is_array($ma) && $sortkey) {

            // temp ma with sort value, quotes convert key to string in case numeric float
            foreach ($ma as $k=>$a) {
                $temp["$a[$sortkey]"][$k] = $a;
            }

            if ($sortorder == 2) {
                // descending
                krsort($temp);
            } else {
                // ascending
                ksort($temp);
            }

            // blank output multiarray to add to
            $newma = array();

            // add sorted arrays to output array
            foreach ($temp as $sma) {
                $newma += $sma;
            }

            // release memory
            unset($ma, $sma, $temp);

            return $newma;
        }
    }

    // recursively convert string to numeric -- NOT NECESSARY
    public static function stringsToNumericRecursive($array) {
        foreach ($array as &$value) {
            // convert string to int or float
            if (is_string($value) && is_numeric($value)) {
                $value = $value + 0;
            }

            // recursively look in next array
            if (is_array($value)) {
                $value = self::stringsToNumericRecursive($value);
            }
        }
        return $array;
    }

    public static function groupPredicates($viewTable, $predicates) {
        $db_preds = $es_preds = [];

        $validColumns = self::getDbColumns($viewTable);

        // sort predicates - DB vs ES
        foreach ($predicates as $field => $whereData) {
            if (in_array($field, $validColumns)) {
                $db_preds[$field] = $whereData;
            } else {
                $es_preds[$field] = $whereData;
            }
        }

        return [$db_preds, $es_preds];
    }

    public static function getDbColumns($table) {
        // get columns
        $query = "
            SELECT column_name
            FROM INFORMATION_SCHEMA.columns
            WHERE table_name = '$table'";
        $validColumns = \DB::select($query);
        $validColumns = array_column($validColumns, 'column_name');

        return $validColumns;
    }



    #####################################################################################
    # developer / troubleshooting methods ###############################################
    #####################################################################################

    /**
     * Print Query
     *
     * @param Builder $query
     * @param array $params
     * @return void
     */
    public static function pq($query, $params=null) {
        $sql = $query->toSql();
        $sql = str_replace("`", "", $sql);
        $rawSql = vsprintf(str_replace("?", '"%s"', $sql), $query->getBindings()) . ';';
        /*var_dump($rawSql);
        dd($rawSql, $params);*/
        return $rawSql;
    }



    #####################################################################################
    # run es/collection predicates ######################################################
    #####################################################################################

    public static function runEsPredicates($esData, $es_preds) {
        foreach ($es_preds as $field => $preds) {
            $esData = self::applyEsFieldPredicates($esData, $field, $preds);
        }

        return $esData;
    }

    public static function applyEsFieldPredicates($esData, $field, $preds) {
        foreach($preds as $pred) {
            $esData = self::applyEsFieldPredicate($esData, $field, $pred['val'], $pred['op']);
        }
        return $esData;
    }

    public static function applyEsFieldPredicate($esData, $field, $val, $op) {

        // if search field is nested, do a recursive search
        if ( substr_count($field, '.') > 0 ) {
            $esData = self::breakEsSubFieldPredicate($esData, $field, $val, $op);
            return $esData;
        }

        // first filter rows without the field
        $esData = $esData->filter(function ($row) use ($field) {
            return isset($row[$field]);
        });

        switch ($op) {
            case 'equal':
                $esData = $esData->where($field, $val);
                return $esData;
            case 'in':
                return $esData->whereIn($field, $val);
            case 'like':
                $val = trim($val, '%');
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return false !== stripos($row[$field], $val);
                });
                return $esData;
            case 'gt':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] > $val;
                });
                return $esData;
            case 'lt':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] < $val;
                });
                return $esData;
            case 'gte':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] >= $val;
                });
                return $esData;
            case 'lte':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] <= $val;
                });
                return $esData;
            case 'not_equal':
                return $esData->whereNotIn($field, $val);
            case 'not_in':
                return $esData->whereNotIn($field, $val);
            case 'not_like':
                $val = trim($val, '%');
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return false === stripos($row[$field], $val);
                });
                return $esData;
            case 'not_gt':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] <= $val;
                });
                return $esData;
            case 'not_lt':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] >= $val;
                });
                return $esData;
            case 'not_gte':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] < $val;
                });
                return $esData;
            case 'not_lte':
                $esData = $esData->filter(function ($row) use ($field, $val) {
                    return $row[$field] > $val;
                });
                return $esData;
        }
    }

    public static function breakEsSubFieldPredicate($esData, $field, $val, $op) {

        // break string into 2 strings on first '.'
        list($parent, $subfield) = explode('.', $field, 2);

        $esData = $esData->filter(function ($sub) use ($parent, $subfield, $val, $op) {

            $subEsData = collect($sub[$parent]);

            // recursively call method to apply predicate to nested array
            $subEsData = self::applyEsFieldPredicate($subEsData, $subfield, $val, $op);

            return $subEsData->count() > 0;
        });

        return $esData;
    }



    #####################################################################################
    # run database predicates ###########################################################
    #####################################################################################

    public static function runDbPredicates($table, $db_preds) {
        $query = DB::table($table);

        foreach ($db_preds as $field => $preds) {
            $query = self::applyDbFieldPredicates($query, $field, $preds);
        }

        $x = self::pq($query);
        $results = $query->get()->toArray();

        return $results;
    }

    public static function applyDbFieldPredicates($query, $field, $preds) {
        foreach($preds as $pred) {
            $query = self::applyDbFieldPredicate($query, $field, $pred['val'], $pred['op']);
        }
        return $query;
    }

    public static function applyDbFieldPredicate($query, $field, $val, $op) {
        switch ($op) {
            case 'equal':
                return $query->where($field, $val);
            case 'in':
                return $query->whereIn($field, $val);
            case 'like':
                if (!str_contains($val, '%')) {
                    $val = '%' . $val . '%';
                }
                return $query->where($field, 'like', $val);
            case 'gt':
                return $query->where($field, '>', $val);
            case 'lt':
                return $query->where($field, '<', $val);
            case 'gte':
                return $query->where($field, '>=', $val);
            case 'lte':
                return $query->where($field, '<=' ,$val);
            case 'not_equal':
                return $query->where($field, '<>', $val);
            case 'not_in':
                return $query->whereNotIn($field, $val);
            case 'not_like':
                if (!str_contains($val, '%')) {
                    $val = '%' . $val . '%';
                }
                return $query->where($field, 'not like', $val);
            case 'not_gt':
                return $query->where($field, '<=', $val);
            case 'not_lt':
                return $query->where($field, '>=', $val);
            case 'not_gte':
                return $query->where($field, '<', $val);
            case 'not_lte':
                return $query->where($field, '>', $val);
        }
    }

}


