from queue import LifoQueue
from typing import List, Set, Tuple
from pyspawn.graph.Relationship import Relationship
from pyspawn.graph.Table import Table


class GraphBuilder:
    """GraphBuilder puts together a orderes list of tables to delete in such a fashion that foreign key constraints are not violated."""
    ### If any combination of tables have cyclical relationships (i.e. A -> FK -> B, B -> FK -> C and C -> FK -> A) that is handled through 'ALTER TABLE {} NOCHECK CONSTRAINT ALL'
    ### and 'ALTER TABLE {} WITH CHECK CHECK CONSTRAINT ALL' statements for the last constraint the _has_cycle() recursion finds that completes the cicle.
    ### For instance, A -> FK -> B, B -> FK -> C and C -> FK -> A: if C -> FK -> A is the last constraint in the _has_cycle() recursion the C would be NOCHECK CONSTRAINT ALL followed by Delete A, Delete B & Delete C
    ### For instance, A -> FK -> B, B -> FK -> C and C -> FK -> A: if B -> FK -> C is the last constraint in the _has_cycle() recursion the B would be NOCHECK CONSTRAINT ALL followed by Delete C, Delete A & Delete B

    def __init__(self, tables: Set[Table], relationships: Set[Relationship]):
        self.to_delete: list[Table] = []
        self._fill_table_relationships(tables, relationships)
        cyclic_relationships, to_delete = self._find_and_remove_cycles(tables)
        self.cyclic_relationships = list(cyclic_relationships)
        
        while not to_delete.empty():
            self.to_delete.append(to_delete.get())
        


    def _fill_table_relationships(self, tables: Set[Table], relationships: Set[Relationship]):
        """Loops through all relationships and adds existing relationships to the Table.relationships set."""
        for r in relationships:
            parent_table = next((t for t in tables if t == r.parent_table), None)
            reference_table = next((t for t in tables if t == r.referenced_table), None)
            if parent_table != None and reference_table != None and parent_table != reference_table:
                parent_table.relationships.add(Relationship(parent_table, reference_table, r.relationship_name))



    def _find_and_remove_cycles(self, tables: set[Table]) -> tuple[set(), LifoQueue[Table]]:
        """Loops through all tables and, in comination with _has_cycle(), creates a Last In First Out (LIFO) queue of tables to be deleted. Cyclical relations are kept separate for special handling."""
        not_visited: Set[Table] = tables.copy()
        visiting: Set[Table] = set()
        visited: Set[Table] = set()
        cyclic_relationships: Set[Relationship] = set()
        to_delete: LifoQueue[Table] = LifoQueue(-1)

        for t in tables:
            self._has_cycle(t, not_visited, visiting, visited, cyclic_relationships, to_delete)

        return (cyclic_relationships, to_delete)



    def _has_cycle(self, table: Table, not_visited: Set[Table], visiting: Set[Table], visited: Set[Table], cyclic_relationships: Set[Relationship], to_delete: LifoQueue[Table]) -> bool:
        """Looks through a tables relationships and checks if it's a cyclical constraint (i.e. A with FK -> B, B with FK -> C and C with FK -> A).\n
           If the table has no relationships it's immediately pushed to the LIFO-queue deleting. If it has dependencies the dependencies are recursively looped until the end of the chain is found or it's established as a cyclical constraint."""
        if table in visited:
            return False
        
        if table in visiting:
            return True

        not_visited.remove(table)
        visiting.add(table)

        for r in table.relationships:
            if self._has_cycle(r.referenced_table, not_visited, visiting, visited, cyclic_relationships, to_delete):
                cyclic_relationships.add(r)

        visiting.remove(table)
        visited.add(table)
        to_delete.put(table)

        return False



# if __name__ == "__main__":
#     a = Table("dbo", "a")
#     b = Table("dbo", "b")
#     c = Table("dbo", "c")
#     r1 = Relationship(a, b, "a_to_b_rel")
#     r2 = Relationship(b, c, "b_to_c_rel")
#     r3 = Relationship(c, a, "c_to_a_rel") #Making it cyclical
#     #Parent table drop constraint #
#     tables = set([a, b, c])
#     relationships = set([r1, r2, r3])
#     Graph = GraphBuilder(tables, relationships)
#     print("Done!")