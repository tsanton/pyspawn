from dataclasses import dataclass


@dataclass(frozen=True)
class TemporalTable:
    """Temporal table class."""
    schema: str
    table_name: str
    history_table_schema: str
    history_table_name: str


    def table_to_string(self) -> str:
        """string implementations utilized for testning"""
        return f"{self.schema}.{self.table_name}"

 
    def history_table_to_string(self) -> str:
        """string implementations utilized for testning"""
        return f"{self.history_table_schema}.{self.history_table_name}"