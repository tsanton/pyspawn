from typing import List
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspawn._graph.graph_builder import GraphBuilder
    from pyspawn._graph.temporal_table import TemporalTable
    from pyspawn._graph.table import Table
    from pyspawn import Checkpoint
from pyspawn.adapters._db_adapter import DbAdapter

class SqlServerAdapter(DbAdapter):
    _quote_char = '"'

    def __init__(self):
        super().__init__()


    def get_database_name_command_text(self) -> str:
        return "SELECT DB_NAME()"


    def get_tables_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all schema- and table names for scoped schemas and tables."""
        cmd_txt = """
        select 
            s.name SchemaName
            , t.name TableName
        from sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE 1=1
        """
        if len(checkpoint.tables_to_ignore) > 0:
            tables_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.tables_to_ignore])
            cmd_txt += f"AND t.name NOT IN ({tables_to_ignore})\n"

        if len(checkpoint.tables_to_include) > 0:
            tables_to_include = ",".join(["'" + x + "'" for x in checkpoint.tables_to_include])
            cmd_txt += f"AND t.name IN ({tables_to_include})\n"
        
        if len(checkpoint.schemas_to_ignore) > 0:
            schemas_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_ignore])
            cmd_txt += f"AND s.name NOT IN ({schemas_to_ignore})\n"
        
        if len(checkpoint.schemas_to_include) > 0:
            schemas_to_include = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_include])
            cmd_txt += f"AND s.name IN ({schemas_to_include})\n"
        return cmd_txt



    def get_temporal_table_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all temporal table names and schemas with their adjoining historical table- and schema names for selected schemas and tables."""
        cmd_txt = """
        select 
            s.name SchemaName
            , t.name TableName
            , temp_s.name HistoryTableSchema
            , temp_t.name HistoryTableName
        from sys.tables t
        INNER JOIN sys.schemas s on t.schema_id = s.schema_id
        INNER JOIN sys.tables temp_t on t.history_table_id = temp_t.object_id
        INNER JOIN sys.schemas temp_s on temp_t.schema_id = temp_s.schema_id
        WHERE t.temporal_type = 2
        """
        if len(checkpoint.tables_to_ignore) > 0:
            tables_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.tables_to_ignore])
            cmd_txt += f"\nAND t.name NOT IN ({tables_to_ignore})"
        return cmd_txt



    def get_relationship_command_text(self, checkpoint: "Checkpoint") -> str:
        """Build a query that selects out all ForeignKey (FK) to PrimaryKey (PK) relations for selected schemas and tables."""
        cmd_txt = """
        select
            chs.name ChildSchemaName
            , cht.name ChildTableName
            , pas.name ParentSchemaName 
            , pat.name ParentTableName
            , sfk.name ForeighKeyName
        from sys.foreign_keys sfk
        inner join sys.objects pat on sfk.referenced_object_id = pat.object_id
        inner join sys.schemas pas on pat.schema_id = pas.schema_id
        inner join sys.objects cht on sfk.parent_object_id = cht.object_id			
        inner join sys.schemas chs on cht.schema_id = chs.schema_id
        where 1=1
        """
        if len(checkpoint.tables_to_ignore) > 0:
            tables_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.tables_to_ignore])
            cmd_txt += f"AND pat.name NOT IN ({tables_to_ignore})\n"

        if len(checkpoint.tables_to_include) > 0:
            tables_to_include = ",".join(["'" + x + "'" for x in checkpoint.tables_to_include])
            cmd_txt += f"AND pat.name IN ({tables_to_include})\n"
        
        if len(checkpoint.schemas_to_ignore) > 0:
            schemas_to_ignore = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_ignore])
            cmd_txt += f"AND pas.name NOT IN ({schemas_to_ignore})\n"
        
        if len(checkpoint.schemas_to_include) > 0:
            schemas_to_include = ",".join(["'" + x + "'" for x in checkpoint.schemas_to_include])
            cmd_txt += f"AND pas.name IN ({schemas_to_include})\n"
        return cmd_txt



    def get_delete_command_text(self, graph: "GraphBuilder") -> str:
        """Build a query that drops cyclical constraints (if any) and deletes tables in an order that does not violate foreign key constraints."""
        cmd_txt = ""

        for r in graph.cyclic_relationships:
            cmd_txt  += f"ALTER TABLE {r.parent_table.get_full_name(self._quote_char)} NOCHECK CONSTRAINT ALL;\n"
        
        for t in graph.to_delete:
            cmd_txt += f"DELETE {t.get_full_name(self._quote_char)}\n;"

        for r in graph.cyclic_relationships:
            cmd_txt  += f"ALTER TABLE {r.parent_table.get_full_name(self._quote_char)} WITH CHECK CHECK CONSTRAINT ALL;\n"
       
        return cmd_txt



    def get_reseed_command_text(self, tables_to_reset: List["Table"]) -> str:
        """Build a query that reseeds identity columns for tables that are to be reset."""
        tables_to_reset = "', '".join([x.to_string() for x in tables_to_reset])
        cmd_txt = f"""
        DECLARE @Schema sysname = N''                                                                                                     			
        DECLARE @TableName sysname = N''                                                                                                  			
        DECLARE @ColumnName sysname = N''                                                                                                 			
        DECLARE @DoReseed sql_variant = 0																											
        DECLARE @NewSeed bigint = 0                                                                                                       			
        DECLARE @IdentityInitialSeedValue int = 0                                                                                                  
        DECLARE @SQL nvarchar(4000) = N''                                                                                                 			
                                                                                                                                                    
        -- find all non-system tables and load into a cursor                                                                              			
        DECLARE IdentityTables CURSOR FAST_FORWARD                                                                                        			
        FOR                                                                                                                               			
            SELECT  OBJECT_SCHEMA_NAME(t.object_id, db_id()) as schemaName,                                                                        
                    t.name as tableName,                                                                                                           
                    c.name as columnName,                                                                                                          
                    ic.last_value,                                                                                                                 
                    IDENT_SEED(OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name) as identityInitialSeedValue                                
            FROM sys.tables t 																										            
            JOIN sys.columns c ON t.object_id=c.object_id      																                	
            JOIN sys.identity_columns ic on ic.object_id = c.object_id  												                		
            WHERE c.is_identity = 1                                                                                    				            
            AND OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name in ('{tables_to_reset}')                              
        OPEN IdentityTables                                                                                                               			
        FETCH NEXT FROM IdentityTables INTO @Schema, @TableName, @ColumnName, @DoReseed, @IdentityInitialSeedValue                                 
        WHILE @@FETCH_STATUS = 0                                                                                                          			
        BEGIN                                                                                                                         			
        -- reseed the identity only on tables that actually have had a value, otherwise next value will be off-by-one   			            
        -- https://stackoverflow.com/questions/472578/dbcc-checkident-sets-identity-to-0                                                      
            if (@DoReseed is not null)                                                                                                         
                SET @SQL = N'DBCC CHECKIDENT(''' +  @Schema + '.' + @TableName + ''', RESEED, ' + Convert(varchar(max), @IdentityInitialSeedValue - 1) + ')' 
            else                                                                                                                               
                SET @SQL = null	                                                                                                                
            if (@sql is not null) EXECUTE (@SQL)  																								                                                                      
            FETCH NEXT FROM IdentityTables INTO  @Schema, @TableName, @ColumnName  , @DoReseed, @IdentityInitialSeedValue                      
        END                                                                                                                           			
        DEALLOCATE IdentityTables
        """
        return cmd_txt



    def build_turn_off_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        """Build a query that turns off system versioning for system versioned temporal tables."""
        cmd_txt = ""
        for t in temporal_tables:
            cmd_txt += f"alter table {self._quote_char}{t.schema}{self._quote_char}.{self._quote_char}{t.table_name}{self._quote_char} set (SYSTEM_VERSIONING = OFF);\n"
        return cmd_txt
    


    def build_turn_on_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        """Build a query that turns on system versioning for system versioned temporal tables."""
        cmd_txt = ""
        for t in temporal_tables:
            cmd_txt += f"alter table {self._quote_char}{t.schema}{self._quote_char}.{self._quote_char}{t.table_name}{self._quote_char} set (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {self._quote_char}{t.history_table_schema}{self._quote_char}.{self._quote_char}{t.history_table_name}{self._quote_char}));\n"
        return cmd_txt



    def supports_temporal_tables(self) -> bool:
        """Indicate if the DBAdapter supports temporal tables."""
        return True