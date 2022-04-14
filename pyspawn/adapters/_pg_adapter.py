from typing import List
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspawn._graph.graph_builder import GraphBuilder
    from pyspawn._graph.temporal_table import TemporalTable
    from pyspawn._graph.table import Table
    from pyspawn import Checkpoint
from pyspawn.adapters._db_adapter import DbAdapter

class PgAdapter(DbAdapter):
    _quote_char = '"'

    def __init__(self):
        super().__init__()

    def get_temporal_table_command_text(self, checkpoint: "Checkpoint") -> str:
        pass

    def get_relationship_command_text(self, checkpoint: "Checkpoint") -> str:
        pass

    def get_delete_command_text(self, graph: "GraphBuilder") -> str:
        pass

    def get_reseed_command_text(self, tables_to_reset: List["Table"]) -> str:
        pass

    def build_turn_off_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        pass

    def build_turn_on_system_versioning_command_text(self, temporal_tables: List["TemporalTable"]) -> str:
        pass

    def supports_temporal_tables(self) -> bool:
        pass

    def get_tables_command_text(self, checkpoint: "Checkpoint") -> str:
        pass

