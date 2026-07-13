from itertools import product
from rdkit import Chem


def _attach(scaffold_smiles, r_group_combo):
    """Zip one r-group onto each attachment point of the scaffold via molzip."""
    scaf = Chem.MolFromSmiles(scaffold_smiles)
    if scaf is None:
        return None
    idx = 1
    for atom in scaf.GetAtoms():
        if atom.GetAtomicNum() == 0:          # dummy [*]
            atom.SetAtomMapNum(idx)
            idx += 1
    combined = scaf
    for i, rg in enumerate(r_group_combo, start=1):
        rmol = Chem.MolFromSmiles(rg)
        if rmol is None:
            return None
        for atom in rmol.GetAtoms():
            if atom.GetAtomicNum() == 0:
                atom.SetAtomMapNum(i)
        combined = Chem.CombineMols(combined, rmol)
    try:
        zipped = Chem.molzip(combined)
        return Chem.MolToSmiles(zipped)
    except Exception:
        return None


def generate_molecules(scaffold_smiles_list, r_group_smiles_list):
    r_valid = [s for s in r_group_smiles_list if Chem.MolFromSmiles(s)]
    out = set()
    for scaf in scaffold_smiles_list:
        m = Chem.MolFromSmiles(scaf)
        if m is None:
            continue
        n_points = sum(1 for a in m.GetAtoms() if a.GetAtomicNum() == 0)
        if n_points == 0:
            out.add(Chem.MolToSmiles(m))
            continue
        for combo in product(r_valid, repeat=n_points):
            smi = _attach(scaf, combo)
            if smi:
                out.add(smi)
    return sorted(out)
