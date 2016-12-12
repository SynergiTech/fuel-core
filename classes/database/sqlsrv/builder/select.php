<?php
namespace Fuel\Core;

class Database_Sqlsrv_Builder_Select extends \Database_Query_Builder_Select
{

	public function compile($db = null)
	{
		if ( ! $db instanceof \Database_Connection)
		{
			// Get the database instance
			$db = $this->_connection ?: \Database_Connection::instance($db);
		}

		// Callback to quote identifiers
		$quote_ident = array($db, 'quote_identifier');

		// Callback to quote tables
		$quote_table = array($db, 'quote_table');

		// Start a selection query
		$query = 'SELECT ';

		if ($this->_distinct === TRUE)
		{
			// Select only unique results
			$query .= 'DISTINCT ';
		}

		if (empty($this->_select))
		{
			// Select all columns
			$query .= '*';
		}
		else
		{
			// Select all columns
			$query .= implode(', ', array_unique(array_map($quote_ident, $this->_select)));
		}

		if ( ! empty($this->_from))
		{
			// Set tables to select from
			$query .= ' FROM '.implode(', ', array_unique(array_map($quote_table, $this->_from)));
		}

		if ( ! empty($this->_join))
		{
			// Add tables to join
			$query .= ' '.$this->_compile_join($db, $this->_join);
		}

		if ( ! empty($this->_where))
		{
			// Add selection conditions
			$query .= ' WHERE '.$this->_compile_conditions($db, $this->_where);
		}

		if ( ! empty($this->_group_by))
		{
			// Add sorting
			$query .= ' GROUP BY '.implode(', ', array_map($quote_ident, $this->_group_by));
		}

		if ( ! empty($this->_having))
		{
			// Add filtering conditions
			$query .= ' HAVING '.$this->_compile_conditions($db, $this->_having);
		}

		if ( ! empty($this->_order_by))
		{
			// Add sorting
			$query .= ' '.$this->_compile_order_by($db, $this->_order_by);
		}
		elseif ($this->_offset !== NULL || $this->_limit !== NULL)
		{
			// MS SQL cannot handle LIMIT without both OFFSET and ORDER BY
			// - could use something like this but not ideal:
			//		$query .= ' '.$this->_compile_order_by($db, array('id', 'ASC'));
			//if ($this->_limit > 1) {
			//	//hope they are using the ORM get_one function
			//	throw new Exception???
			//}
		}

		if ($this->_offset !== NULL and $this->_offset > 0)
		{
			// Add offsets
			$query .= ' OFFSET '.$this->_offset. ' ROWS';
		}

		if ($this->_limit !== NULL and ! empty($this->_order_by))
		{
			// MS SQL needs both OFFSET and ORDER BY to apply a limit
			if ($this->_offset === NULL)
			{
				$query .= ' OFFSET 0 ROWS';
			}

			// Add limiting
			// - MS SQL cares not for your grammar
			$query .= ' FETCH NEXT '.$this->_limit.' ROWS ONLY';
		}

		return $query;
	}

}