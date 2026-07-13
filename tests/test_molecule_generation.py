from chem_pipeline.molecule_generation import generate_molecules

def test_single_point():
    mols = generate_molecules(["c1ccccc1*"], ["C*", "N*"])
    assert len(mols) == 2

def test_invalid_scaffold_skipped():
    assert generate_molecules(["not_smiles"], ["C*"]) == []
