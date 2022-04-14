# **Pyspawn** #

<p> An intelligent database cleaner for integration tests ported from Jbogard/Respawn

To use, create a Checkpoint and initialize with tables you want to skip, or schemas you want to keep/ignore:

```Python
import pyodbc
from pyspawn import Checkpoint
from pyspawn import SqlServerAdapter

Chkpt = Checkpoint(
    tables_to_ignore=["A"],
    tables_to_include=["B", "C"],
    schemas_to_ignore=["schema1"],
    schemas_to_include=["schema2", "schema3"],
    check_temporal_table=False,
    reseed_identity=True,
    db_adapter=SqlServerAdapter()
)
```

In your tests, in the fixture setup, reset your checkpoint:

```Python
import pyodbc
from pyspawn import Checkpoint
from pyspawn import SqlServerAdapter

with pyodbc.connect(con_str) as conn:
    Chkpt = Checkpoint(
        tables_to_include=["A", "B", "C"],
        schemas_to_include=["dbo"]
    db_adapter = SqlServerAdapter()
    )
    Chkpt.reset(conn)
```

## **How does it work?** ##

Pyspawn examines the SQL metadata intelligently to build a deterministic order of tables to delete based on foreign key relationships between tables. It navigates these relationships to build a DELETE script starting with the tables with no relationships and moving inwards until all tables are accounted for.

Once this in-order list of tables is created, the Checkpoint object keeps this list of tables privately so that the list of tables and the order is only calculated once.

In your tests, you Reset your checkpoint before each test run. If there are any tables/schemas that you don't want to be cleared out, include these in the configuration of your Checkpoint.

In benchmarks, a deterministic deletion of tables is faster than truncation, since truncation requires disabling or deleting foreign key constraints. Deletion results in easier test debugging/maintenance, as transaction rollbacks/post-test deletion still rely on that mechanism at the beginning of each test. If data comes in from another source, your test might fail. Respawning to your checkpoint assures you have a known starting point before each test.

## **Installing Pyspawn** ##

You should install Pyspawn with pip:

```
pip install pyspawn 
```


## **Want to contribute?** ##

### **Local Development** ###

The repo ships with a .devcontainer-setup. Adding a .env-file with the following variables will let you spin up a Python dev-environment along with a MSSQL database ready for use:
- PYTHONHASHSEED=0
- ACCEPT_EULA=Y
- SA_PASSWORD=Sup3rStrong?Password

### **Testing** ###

To run tests docker-compose is dependent on a few environment variables from a .env-file located in the root directory.
- "PYTHONHASHSEED=0" to avoid runtime randomization of hash seed. Randomization will mess up stable hashing order, altering return orders of sets and by that fail tests.
- "ACCEPT_EULA=Y" to spin up a MS-SQL database for the integration tests
- "SA_PASSWORD=*YourPassword*" for the SQL server SA-login

When the .env-file is in place all you have to run is:
```bash
docker-compose -f docker-compose.yaml -f docker-compose.testing.yaml build
docker-compose -f docker-compose.yaml -f docker-compose.testing.yaml run --rm testsuite
```

## **Honors** ##

Lastly I'd like to send my thanks to Jbogard for creating Respawn in the first place! I've found it to be a fantastic tool to greatly improve the testing experience and general code quality. 