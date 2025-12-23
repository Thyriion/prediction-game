# `import_openligadb` Management Command

This Django management command imports football match data from **OpenLigaDB** into the local database.

It supports a **full bootstrap import** and a **smart incremental update** and is designed to be **idempotent**, **safe to run repeatedly**, and **cron-friendly**.

---

## Command Usage

```bash
python manage.py import_openligadb
```

### Required Arguments

- `--league`  
	League shortcut as used by OpenLigaDB.  
	Examples:  
	- `--league bl1`   # Bundesliga 1  
	- `--league bl2`   # Bundesliga 2

- `--mode`  
	Import mode.  
	Examples:  
	- `--mode bootstrap`   # full import of a season  
	- `--mode smart`       # incremental update (recommended for cron)

### Optional Arguments

- `--season`  
	Season year to import (e.g. 2025).  
	If omitted, the command automatically detects the active season using OpenLigaDB match data (league-agnostic).  
	Example:  
	- `--season 2025`

- `--dry-run`  
	Simulate the import without writing anything to the database.  
	Example:  
	- `--dry-run`

- `--timeout`  
	HTTP timeout for OpenLigaDB API requests (in seconds).  
	Default: 10  
	Example:  
	- `--timeout 20`

---

## Examples

**Bootstrap a season (one-time or after database reset):**
```bash
python manage.py import_openligadb --league bl1 --mode bootstrap
```

**Smart update with automatic season detection:**
```bash
python manage.py import_openligadb --league bl1 --mode smart
```

**Dry-run (no database writes):**
```bash
python manage.py import_openligadb --league bl1 --mode smart --dry-run
```

---

## Typical Workflow

1. **Once per season (or after database reset):**
		```bash
		python manage.py import_openligadb --league bl1 --mode bootstrap
		```
2. **Regular updates (cron):**
		```bash
		python manage.py import_openligadb --league bl1 --mode smart
		```

---

## Cron Recommendation

```cron
*/15 * * * * /path/to/venv/bin/python /path/to/project/manage.py import_openligadb --league bl1 --mode smart
```

---