<?php

namespace Fuel\Core;

class Database_Sqlsrv_Schema extends \Database_Schema
{

	public function __construct($name, $connection)
	{
		parent::__construct($name, $connection);
	}

	/*public function create_database($database, $charset = null, $if_not_exists = true)
	{

	}

	public function drop_database($database)
	{

	}*/

	public function drop_table($table)
	{
		$table = $this->_connection->quote_identifier($this->_connection->table_prefix($table));

		$sql  = 'IF EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '.str_replace('"', "'", $table).")\n";
		$sql .= 'DROP TABLE '.$table;

		return $this->_connection->query(0, $sql, false);
	}

	/*public function rename_table($table, $new_table_name)
	{

	}*/

	public function create_table($table, $fields, $primary_keys = array(), $if_not_exists = true, $engine = false, $charset = null, $foreign_keys = array())
	{
		$table = $this->_connection->quote_identifier($this->_connection->table_prefix($table));

		$sql = "";

		if ($if_not_exists)
		{
			$sql .= 'IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '.str_replace('"', "'", $table).")\n";
		}

		$sql .= 'CREATE TABLE '.$table." (";

		$sql .= $this->process_fields($fields, null);

		if ( ! empty($primary_keys))
		{
			foreach ($primary_keys as $index => $primary_key)
			{
				$primary_keys[$index] = $this->_connection->quote_identifier($primary_key);
			}
			$sql .= ",\n\tPRIMARY KEY (".implode(', ', $primary_keys).')';
		}

		if ( ! empty($foreign_keys))
		{
			$sql .= $this->process_foreign_keys($foreign_keys);
		}

		/*$sql .= "\n)";
		$sql .= ($engine !== false) ? ' ENGINE = '.$engine.' ' : '';
		$sql .= $this->process_charset($charset, true).";";*/

		$sql .= "\n)";

		return $this->_connection->query(0, $sql, false);
	}

	/*public function truncate_table($table)
	{

	}

	public function table_exists($table)
	{

	}*/

	public function field_exists($table, $columns)
	{
		if ( ! is_array($columns))
		{
			$columns = array($columns);
		}

		$sql  = 'SELECT ';
		$sql .= implode(', ', array_unique(array_map(array($this->_connection, 'quote_identifier'), $columns)));
		$sql .= ' FROM ';
		$sql .= $this->_connection->quote_identifier($this->_connection->table_prefix($table));
		// OMIT THE LIMIT 1 FOR MS SQL

		try
		{
			$this->_connection->query(\DB::SELECT, $sql, false);
			return true;
		}
		catch (\Database_Exception $e)
		{
			// check if we have a DB connection at all
			if ( ! $this->_connection->has_connection())
			{
				// if no connection could be made, re throw the exception
				throw $e;
			}

			return false;
		}
	}

	/*public function create_index($table, $index_columns, $index_name = '', $index = '')
	{

	}*/

	public function drop_index($table, $index_name)
	{
		if (strtoupper($index_name) == 'PRIMARY')
		{
			$table = str_replace('"', "'", $this->_connection->quote_identifier($this->_connection->table_prefix($table)));
			$sql = "DECLARE @table NVARCHAR(512), @sql NVARCHAR(MAX);

					SELECT @table = N$table;

					SELECT @sql = 'ALTER TABLE ' + @table + ' DROP CONSTRAINT ' + name + ';'
					FROM sys.key_constraints
					WHERE [type] = 'PK'
					AND [parent_object_id] = OBJECT_ID(@table);

					EXEC sp_executeSQL @sql;";
		}
		else
		{
			$sql  = 'DROP INDEX '.$this->_connection->quote_identifier($index_name);
			$sql .= ' ON '.$this->_connection->quote_identifier($this->_connection->table_prefix($table));
		}

		return $this->_connection->query(0, $sql, false);
	}

	/*public function add_foreign_key($table, $foreign_key)
	{

	}

	public function drop_foreign_key($table, $fk_name)
	{

	}*/

	public function process_foreign_keys($foreign_keys)
	{
		if ( ! is_array($foreign_keys))
		{
			throw new \Database_Exception('Foreign keys on create_table() must be specified as an array');
		}

		$fk_list = array();

		foreach($foreign_keys as $definition)
		{
			// some sanity checks
			if (empty($definition['key']))
			{
				throw new \Database_Exception('Foreign keys on create_table() must specify a foreign key name');
			}
			if ( empty($definition['reference']))
			{
				throw new \Database_Exception('Foreign keys on create_table() must specify a foreign key reference');
			}
			if (empty($definition['reference']['table']) or empty($definition['reference']['column']))
			{
				throw new \Database_Exception('Foreign keys on create_table() must specify a reference table and column name');
			}

			$sql = '';
			! empty($definition['constraint']) and $sql .= " CONSTRAINT ".$this->_connection->quote_identifier($definition['constraint']);
			$sql .= " FOREIGN KEY (".$this->_connection->quote_identifier($definition['key']).')';
			$sql .= " REFERENCES ".$this->_connection->quote_identifier($this->_connection->table_prefix($definition['reference']['table'])).' (';
			if (is_array($definition['reference']['column']))
			{
				$sql .= implode(', ', $this->_connection->quote_identifier($definition['reference']['column']));
			}
			else
			{
				$sql .= $this->_connection->quote_identifier($definition['reference']['column']);
			}
			$sql .= ')';
			! empty($definition['on_update']) and $sql .= " ON UPDATE ".$definition['on_update'];
			! empty($definition['on_delete']) and $sql .= " ON DELETE ".$definition['on_delete'];

			$fk_list[] = "\n\t".ltrim($sql);
		}

		return ', '.implode(',', $fk_list);
	}

	public function alter_fields($type, $table, $fields)
	{
		// when altering a table, mssql specifies the type once for the whole query, mysql specifies it on each line
		$sql = 'ALTER TABLE '.$this->_connection->quote_identifier($this->_connection->table_prefix($table)).' ';

		if ($type === 'DROP')
		{
			if ( ! is_array($fields))
			{
				$fields = array($fields);
			}

			$drop_fields = array();
			foreach ($fields as $field)
			{
				$drop_fields[] = $this->_connection->quote_identifier($field);
			}
			$sql .= 'DROP COLUMN '.implode(', ', $drop_fields);
		}
		else
		{
			$use_brackets = ! in_array($type, array('ADD', 'CHANGE', 'MODIFY'));
			$use_brackets and $sql .= $type.' ';
			$use_brackets and $sql .= '(';
			$sql .= (( ! $use_brackets) ? $type.' ' : '') . $this->process_fields($fields);
			$use_brackets and $sql .= ')';
		}

		return $this->_connection->query(0, $sql, false);
	}

	/*public function table_maintenance($operation, $table)
	{

	}

	protected function process_charset($charset = null, $is_default = false, $collation = null)
	{

	}*/

	protected function process_fields($fields, $prefix = '')
	{
		$sql_fields = array();

		foreach ($fields as $field => $attr)
		{
			$attr = array_change_key_case($attr, CASE_UPPER);
			$_prefix = $prefix;

			if (array_key_exists('TYPE', $attr))
			{
				// MS SQL has no direct match for ENUM, should use an extra database table instead really
				if ($attr['TYPE'] == 'enum')
				{
					$attr['TYPE']       = 'varchar';
					$attr['CONSTRAINT'] = '255';
				}
				// MS SQL has no tinytext which has the same properties as a full varchar
				elseif ($attr['TYPE'] == 'tinytext')
				{
					$attr['TYPE']       = 'varchar';
					$attr['CONSTRAINT'] = '255';
				}
				// MS SQL has no longtext, use the longest possible varchar
				elseif ($attr['TYPE'] == 'longtext')
				{
					$attr['TYPE']       = 'varchar';
					$attr['CONSTRAINT'] = 'max';
				}
			}

			if ($_prefix === 'MODIFY ')
			{
				if (array_key_exists('NAME', $attr) and $field !== $attr['NAME'])
				{
					$_prefix = 'CHANGE ';
				}
				else
				{
					$_prefix = 'ALTER COLUMN ';
				}
			}
			$sql  = "\n\t".$_prefix;
			$sql .= $this->_connection->quote_identifier($field);
			if (array_key_exists('NAME', $attr) and $attr['NAME'] !== $field)
			{
				$sql .= ' '.$this->_connection->quote_identifier($attr['NAME']).' ';
			}
			if (array_key_exists('TYPE', $attr))
			{
				$sql .= ' '.$attr['TYPE'];
			}

			// MS SQL cannot apply a field length to integers
			if (array_key_exists('CONSTRAINT', $attr) and substr($attr['TYPE'], -3) !== 'int')
			{
				if (is_array($attr['CONSTRAINT']))
				{
					$sql .= "(";
					foreach($attr['CONSTRAINT'] as $constraint)
					{
						$sql .= (is_string($constraint) ? "'".$constraint."'" : $constraint).', ';
					}
					$sql = rtrim($sql, ', '). ')';
				}
				else
				{
					$sql .= '('.$attr['CONSTRAINT'].')';
				}
			}

			if (array_key_exists('CHARSET', $attr))
			{
				$sql .= $this->process_charset($attr['CHARSET'], false);
			}

			// MS SQL does not support changing default value as part of a query, must be a separate line somehow
			if (array_key_exists('DEFAULT', $attr) and $_prefix !== "ALTER COLUMN ")
			{
				$sql .= ' DEFAULT '.(($attr['DEFAULT'] instanceof \Database_Expression) ? $attr['DEFAULT']  : $this->_connection->quote($attr['DEFAULT']));
			}

			if (array_key_exists('NULL', $attr) and $attr['NULL'] === true)
			{
				$sql .= ' NULL';
			}
			else
			{
				$sql .= ' NOT NULL';
			}

			if (array_key_exists('AUTO_INCREMENT', $attr) and $attr['AUTO_INCREMENT'] === true)
			{
				$sql .= ' IDENTITY(1,1)';
			}

			if (array_key_exists('PRIMARY_KEY', $attr) and $attr['PRIMARY_KEY'] === true)
			{
				$sql .= ' PRIMARY KEY';
			}

			$sql_fields[] = $sql;
		}

		return implode(',', $sql_fields);
	}
}
