from pyspawn.graph.GraphBuilder import GraphBuilder
from pyspawn.graph.Table import Table
from pyspawn.graph.Relationship import Relationship


### NB! Environment variable "PYTHONHASHSEED" has to be set to "0" to avoid runtime randomization of hash seed. Randomization will mess up return order and fail tests ###

def test_delete_list_with_one_table():
    ### Arrange ###
    A = Table("dbo", "A")
    Tables = [A]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set())

    ### Assert ###
    assert Builder.to_delete == Tables, "Results not as expected"



def test_delete_list_with_two_unrelated_tables():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    Tables = [A, B]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set())

    ### Assert ###
    assert Builder.to_delete == [B, A], "Results not as expected"



def test_delete_list_two_tables_one_foreign_key_relationship():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    Tables = [A, B]
    A_to_B = Relationship(A, B, "A.B")
    Relationships = [A_to_B]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.to_delete == [A, B], "Results not as expected"



def test_delete_list_three_tables_two_foreign_key_relationships():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    Tables = [A, B, C]
    A_to_B = Relationship(A, B, "A.B")
    B_to_C = Relationship(B, C, "B.C")
    Relationships = [A_to_B, B_to_C]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.to_delete == [A, B, C], "Results not as expected"



def test_delete_list_three_tables_three_non_cyclical_foreign_key_relationships():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    Tables = [A, B, C]
    A_to_B = Relationship(A, B, "A.B")
    A_to_C = Relationship(A, C, "A.C")
    B_to_C = Relationship(B, C, "B.C")
    Relationships = [A_to_B, A_to_C, B_to_C]
    
    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.to_delete == [A, B, C], "Results not as expected"



def test_delete_list_three_tables_with_equivalent_relationships():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    Tables = [A, B, C]
    A_to_B = Relationship(A, B, "A.B")
    A_to_C = Relationship(A, C, "A.C")
    B_to_C1 = Relationship(B, C, "B.C1")
    B_to_C2 = Relationship(B, C, "B.C2")
    Relationships = [A_to_B, A_to_C, B_to_C1, B_to_C2]
    
    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.to_delete == [A, B, C], "Results not as expected"



def test_delete_list_four_tables_with_two_disparate_relationships():
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    Tables = [A, B, C, D]
    A_to_B = Relationship(A, B, "A.B")
    C_to_D = Relationship(C, D, "C.D")
    Relationships = [A_to_B, C_to_D]
    
    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.to_delete == [A, B, C, D], "Results not as expected"



def test_remove_circular_cycle():
    """A and B have circular relationships. Therefore you have to set NOCHECK on one of the tables (A or B) and delete from the opposite table first."""
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    Tables = [A, B]
    A_to_B = Relationship(A, B, "A.B")
    B_to_A = Relationship(B, A, "B.A")
    Relationships = [A_to_B, B_to_A]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [B_to_A], "Not returning expected circular relationship"
    assert Builder.to_delete == [A, B], "Results not as expected"
    


def test_ignore_self_reference():
    A = Table("dbo", "A")

    Tables = [A]
    A_to_A = Relationship(A, A, "A.A")
    Relationships = [A_to_A]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [], "Not returning expected circular relationship"
    assert Builder.to_delete == [A], "Results not as expected"
    


def test_remove_cyclical_relationship_ignore_normal_relationships():
    """C and D have cyclical relationships. Therefore you have to set NOCHECK on one of the tables (C or D) and delete from the opposite table first."""
    ### Arrange ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    Tables = [A,B,C,D]
    A_to_B = Relationship(A, B, "A.B")
    B_to_C = Relationship(B, C, "B.C")
    C_to_D = Relationship(C, D, "C.D")
    D_to_C = Relationship(D, C, "D.C")
    Relationships = [A_to_B, B_to_C, C_to_D, D_to_C]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [C_to_D], "Not returning expected cyclical relationship"
    assert Builder.to_delete == [A, B, D, C], "Results not as expected"
    


def test_remove_cycles_withouth_removing_start():
    """B and C have cyclical relationships. Therefore you have to set NOCHECK on one of the tables (B or C) and delete from the opposite table first."""
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    Tables = [A,B,C]
    A_to_B = Relationship(A, B, "A.B")
    B_to_C = Relationship(B, C, "B.C")
    C_to_B = Relationship(C, B, "C.B")
    Relationships = [A_to_B, B_to_C, C_to_B]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [B_to_C], "Not returning expected cyclical relationship"
    assert Builder.to_delete == [A, C, B], "Results not as expected"
    


def test_find_one_cyclical_relationship():
    """A and B have cyclical relationships. Therefore you have to set NOCHECK on one of the tables (A or B) and delete from the opposite table first."""
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    E = Table("dbo", "E")
    F = Table("dbo", "F")
    Tables = [A,B,C,D,E,F]
    A_to_B = Relationship(A, B, "A.B")
    B_to_A = Relationship(B, A, "B.A")
    B_to_C = Relationship(B, C, "B.C")
    B_to_D = Relationship(B, D, "B.D")
    C_to_D = Relationship(C, D, "C.D")
    E_to_A = Relationship(E, A, "E.A")
    F_to_B = Relationship(F, B, "F.B")
    Relationships = [A_to_B, B_to_A, B_to_C, B_to_D, C_to_D, E_to_A, F_to_B]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [B_to_A], "Not returning expected cyclical relationship"
    assert Builder.to_delete == [F,E,A,B,C,D], "Results not as expected"
    


def test_multiple_simple_cyclical_relationships():
    ### Act ###
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    Tables = [A,B,C,D,]
    A_to_B = Relationship(A, B, "A.B")
    B_to_A = Relationship(B, A, "B.A")
    C_to_D = Relationship(C, D, "C.D")
    D_to_C = Relationship(D, C, "D.C")
    Relationships = [A_to_B, B_to_A, C_to_D, D_to_C]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [C_to_D, B_to_A], "Not returning expected cyclical relationship"
    assert Builder.to_delete == [A, B, D, C], "Results not as expected"
    


def test_find_multiple_cyclical_relationship():
    """We have the following cyclical relationships:
        - B -> C -> D -> B
        - D -> E -> F -> D
        - E -> D
    """
    A = Table("dbo", "A")
    B = Table("dbo", "B")
    C = Table("dbo", "C")
    D = Table("dbo", "D")
    E = Table("dbo", "E")
    F = Table("dbo", "F")
    Tables = [A,B,C,D,E,F]
    A_to_B = Relationship(A, B, "A.B")
    B_to_C = Relationship(B, C, "B.C")#
    C_to_D = Relationship(C, D, "C.D")#
    D_to_B = Relationship(D, B, "D.B")#
    D_to_E = Relationship(D, E, "D.E")
    E_to_D = Relationship(E, D, "E.D")
    E_to_F = Relationship(E, F, "E.F")
    F_to_D = Relationship(F, D, "F.D")
    Relationships = [A_to_B, B_to_C, C_to_D, D_to_B, D_to_E, E_to_D, E_to_F, F_to_D]

    ### Act ###
    Builder = GraphBuilder(set(Tables), set(Relationships))

    ### Assert ###
    assert Builder.cyclic_relationships == [C_to_D, E_to_D, F_to_D], "Not returning expected cyclical relationship"
    assert Builder.to_delete == [A,D,B,C,E,F], "Results not as expected"
    

    
if __name__ == "__main__":
    print("Starting Graph tests")
    test_delete_list_with_one_table()
    test_delete_list_with_two_unrelated_tables()
    test_delete_list_two_tables_one_foreign_key_relationship()
    test_delete_list_three_tables_two_foreign_key_relationships()
    test_delete_list_three_tables_three_non_cyclical_foreign_key_relationships()
    test_delete_list_three_tables_with_equivalent_relationships()
    test_delete_list_four_tables_with_two_disparate_relationships()
    test_remove_simple_cycle()
    test_ignore_self_reference()
    test_remove_cyclical_relationship_ignore_normal_relationships()
    test_remove_cycles_withouth_removing_start()
    test_find_one_cyclical_relationship()
    test_multiple_simple_cyclical_relationships()
    test_find_multiple_cyclical_relationship()
    print("Done")