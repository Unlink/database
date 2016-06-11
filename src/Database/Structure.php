<?php

/**
 * This file is part of the Nette Framework (https://nette.org)
 * Copyright (c) 2004 David Grudl (https://davidgrudl.com)
 */

namespace Nette\Database;

use Nette;


/**
 * Cached reflection of database structure.
 */
class Structure implements IStructure
{
	use Nette\SmartObject;

	/** @var Connection */
	protected $connection;

	/** @var Nette\Caching\Cache */
	protected $cache;

	/** @var array */
	protected $structure;

	/** @var bool */
	protected $isRebuilt = FALSE;


	public function __construct(Connection $connection, Nette\Caching\IStorage $cacheStorage)
	{
		$this->connection = $connection;
		$this->cache = new Nette\Caching\Cache($cacheStorage, 'Nette.Database.Structure.' . md5($this->connection->getDsn()));
	}


	public function getTables()
	{
		$this->needStructure();
		$tables = [];
		foreach ($this->structure['tables'] as $name => $meta) {
			$tables[] = [
				'name' => isset($meta['name']) ? $meta['name'] : $name,
				'view' => isset($meta['view']) ? $meta['view'] : FALSE
			];
		}
		return $tables;
	}


	public function getPrimaryKey($table)
	{
		$this->needStructure();
		$table = $this->resolveFQTableName($table);

		if (!isset($this->structure['tables'][$table]['primary'])) {
			return NULL;
		}

		return $this->structure['tables'][$table]['primary'];
	}


	public function getPrimaryKeySequence($table)
	{
		$this->needStructure();
		$table = $this->resolveFQTableName($table);

		if (!$this->connection->getSupplementalDriver()->isSupported(ISupplementalDriver::SUPPORT_SEQUENCE)) {
			return NULL;
		}

		$primary = $this->getPrimaryKey($table);
		if (!$primary || is_array($primary)) {
			return NULL;
		}

		dump($this->structure['tables'][$table]);

		if (!isset($this->structure['tables'][$table]['sequence'])) {
			return NULL;
		}

		return $this->structure['tables'][$table]['sequence']['name'];
	}


	public function getHasManyReference($table, $targetTable = NULL)
	{
		$this->needStructure();
		$table = $this->resolveFQTableName($table);

		if ($targetTable) {
			$targetTable = $this->resolveFQTableName($targetTable);
			foreach ($this->structure['hasMany'][$table] as $key => $value) {
				if (strtolower($key) === $targetTable) {
					return $this->structure['hasMany'][$table][$key];
				}
			}

			return NULL;

		} else {
			if (!isset($this->structure['hasMany'][$table])) {
				return [];
			}
			return $this->structure['hasMany'][$table];
		}
	}


	public function getBelongsToReference($table, $column = NULL)
	{
		$this->needStructure();
		$table = $this->resolveFQTableName($table);

		if ($column) {
			$column = strtolower($column);
			if (!isset($this->structure['belongsTo'][$table][$column])) {
				return NULL;
			}
			return $this->structure['belongsTo'][$table][$column];

		} else {
			if (!isset($this->structure['belongsTo'][$table])) {
				return [];
			}
			return $this->structure['belongsTo'][$table];
		}
	}


	public function rebuild()
	{
		$this->structure = $this->loadStructure();
		$this->cache->save('structure', $this->structure);
	}


	public function isRebuilt()
	{
		return $this->isRebuilt;
	}


	protected function needStructure()
	{
		if ($this->structure !== NULL) {
			return;
		}

		$this->structure = $this->cache->load('structure', [$this, 'loadStructure']);
	}


	/**
	 * @internal
	 */
	public function loadStructure()
	{
		$driver = $this->connection->getSupplementalDriver();

		$structure = [];

		foreach ($driver->getTables() as $tablePair) {
			if (isset($tablePair['fullName'])) {
				$table = $tablePair['fullName'];
				$structure['aliases'][strtolower($tablePair['name'])] = strtolower($table);
			} else {
				$table = $tablePair['name'];
			}

			$columns = $driver->getColumns($table);

			if (!$tablePair['view']) {
				$structure['tables'][strtolower($table)] = $this->analyzeColumns($columns);
				$this->analyzeForeignKeys($structure, $table);
			}
			else {
				$structure['tables'][strtolower($table)]['view'] = TRUE;
			}

			if (strtolower($table) !== $table) {
				$structure['tables'][strtolower($table)]['name'] = $table;
			}
		}

		if (isset($structure['hasMany'])) {
			foreach ($structure['hasMany'] as & $table) {
				uksort($table, function ($a, $b) {
					return strlen($a) - strlen($b);
				});
			}
		}

		$this->isRebuilt = TRUE;

		return $structure;
	}


	protected function analyzeColumns(array $columns)
	{
		$tableInfo = [];
		foreach ($columns as $column) {
			if ($column['primary']) {
				$tableInfo['primary'][] = $column['name'];
			}
			if (!isset($column['autoincrement'])) {
				dump($column);
			}
			if ($column['autoincrement']) {
				$tableInfo['autoincrement'] = $column['name'];
			}
			if (isset($column['vendor']['sequence'])) {
				$tableInfo['sequence'] = [
					'column' => $column['name'],
					'name' => $column['vendor']['sequence']
				];
			}
		}

		if (isset($tableInfo['primary']) && count($tableInfo['primary']) === 1) {
			$tableInfo['primary'] = reset($tableInfo['primary']);
		}
		return $tableInfo;
	}


	protected function analyzeForeignKeys(& $structure, $table)
	{
		$lowerTable = strtolower($table);
		foreach ($this->connection->getSupplementalDriver()->getForeignKeys($table) as $row) {
			$structure['belongsTo'][$lowerTable][$row['local']] = $row['table'];
			$structure['hasMany'][strtolower($row['table'])][$table][] = $row['local'];
		}

		if (isset($structure['belongsTo'][$lowerTable])) {
			uksort($structure['belongsTo'][$lowerTable], function ($a, $b) {
				return strlen($a) - strlen($b);
			});
		}
	}


	protected function resolveFQTableName($table)
	{
		$name = strtolower($table);
		if (isset($this->structure['tables'][$name])) {
			return $name;
		}

		if (isset($this->structure['aliases'][$name])) {
			return $this->structure['aliases'][$name];
		}

		if (!$this->isRebuilt()) {
			$this->rebuild();
			return $this->resolveFQTableName($table);
		}

		throw new Nette\InvalidArgumentException("Table '$name' does not exist.");
	}

}
