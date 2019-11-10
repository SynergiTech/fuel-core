<?php

namespace Fuel\Core\Migrate;

class Migration
{
	private $name;
	private $type;
	private $location;
	private $file_name;
	private $class_name;
	private $version;
	private $name_format = 'new';

	public static function from_db($row)
	{
		return new self($row['migration'], null, $row['type'], $row['name']);
	}

	public static function get_comparable_value($value)
	{
		$datetime = \DateTime::createFromFormat(\Migrate::get_timestamp_format(), $value, new \DateTimeZone('UTC'));
		if ($datetime === false) {
			$datetime = new \DateTime("@$value", new \DateTimeZone('UTC'));
		}

		return $datetime->getTimestamp();
	}

	public function __construct($file_name, $location = null, $type = 'app', $name = 'default')
	{
		$this->location = $location;
		$this->file_name = $file_name;
		$this->type = $type;
		$this->name = $name;
		if ($location) {
			$this->file_name = basename($location, '.php');
		}

		if (preg_match('/^([0-9]+_[0-9]+_[0-9]+_[0-9]+)_(.*)/', $this->file_name, $match)) {
			// new-style
			$this->version = $match[1];
			$this->class_name = ucfirst($match[2]);
		} elseif (preg_match('/^(.*?)_(.*)/', $this->file_name, $match)) {
			// old-style
			$this->version = $match[1];
			$this->class_name = ucfirst($match[2]);
			$this->name_format = 'old';
		}

		if (!$this->version or !$this->class_name) {
			throw new \FuelException(sprintf('Invalid migration filename "%s"', $this->file_name));
		}
	}

	public function get_file_name()
	{
		return $this->file_name;
	}

	public function get_location()
	{
		return $this->location;
	}

	public function get_version()
	{
		return $this->version;
	}

	public function get_version_comparable()
	{
		return $this->version;
	}

	public function get_class_name()
	{
		return $this->class_name;
	}

	public function get_type()
	{
		return $this->type;
	}

	public function get_name()
	{
		return $this->name;
	}

	public function hasOldNameFormat()
	{
		return $this->name_format === 'old';
	}

	public function load($namespace = '')
	{
		if (!$this->location) {
			throw new \FuelException(sprintf('Cannot load migration without physical path', $this->get_location(), $class));
		}
		include_once $this->get_location();

		$class = $namespace.$this->get_class_name();

		// make sure it exists in the migration file loaded
		if ( ! class_exists($class, false))
		{
			throw new \FuelException(sprintf('Migration "%s" does not contain expected class "%s"', $this->get_location(), $class));
		}

		// and that it contains an "up" and "down" method
		if ( ! is_callable(array($class, 'up')) or ! is_callable(array($class, 'down')))
		{
			throw new \FuelException(sprintf('Migration class "%s" must include public methods "up" and "down"', $name));
		}

		return $class;
	}

	public function is_within($start = null, $end = null)
	{
		if ($start !== null) {
			$start = self::get_comparable_value($start);
		}
		if ($end !== null) {
			$end = self::get_comparable_value($end);
		}
		$version = self::get_comparable_value($this->get_version());

		return (($start === null or $version > $start) and ($end === null or $version <= $end));
	}
}
