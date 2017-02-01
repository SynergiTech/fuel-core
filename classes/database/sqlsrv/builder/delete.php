<?php

namespace Fuel\Core;

class Database_Sqlsrv_Builder_Delete extends \Database_Query_Builder_Delete
{

	public function compile($db = null)
	{
		// MS SQL cannot handle limits when deleting, nullify here
		// - ORM applies a limit when deleting a single row
		$this->_limit = null;
		return parent::compile($db);
	}
}