from rdkit import Chem


def check_input(df, required_column="smiles"):
    errors = []
    if required_column not in df.columns:
        return [f"missing column '{required_column}'"]
    if df.empty:
        errors.append("file is empty")
    n_null = int(df[required_column].isnull().sum())
    if n_null:
        errors.append(f"{n_null} null SMILES")
    invalid = [s for s in df[required_column].dropna()
               if Chem.MolFromSmiles(str(s)) is None]
    if invalid:
        errors.append(f"{len(invalid)} invalid SMILES (e.g. {invalid[:3]})")
    return errors


def check_output(mol_count, min_expected=1):
    if mol_count < min_expected:
        return [f"only {mol_count} molecules generated (< {min_expected})"]
    return []
