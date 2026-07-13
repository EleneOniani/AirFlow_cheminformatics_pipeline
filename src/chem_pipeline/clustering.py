import numpy as np
from rdkit import Chem
from rdkit.Chem import AllChem, DataStructs
from sklearn.cluster import KMeans


def _fp(smiles, n_bits=2048, radius=2):
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    bv = AllChem.GetMorganFingerprintAsBitVect(mol, radius, nBits=n_bits)
    arr = np.zeros((n_bits,), dtype=np.int8)
    DataStructs.ConvertToNumpyArray(bv, arr)
    return arr


def cluster_molecules(smiles_list, n_clusters=None, random_state=42):
    fps, valid = [], []
    for s in smiles_list:
        f = _fp(s)
        if f is not None:
            fps.append(f); valid.append(s)
    if not valid:
        return {}
    X = np.array(fps)
    if n_clusters is None:
        n_clusters = max(1, int(len(valid) ** 0.5))
    n_clusters = min(n_clusters, len(valid))
    labels = KMeans(n_clusters=n_clusters, random_state=random_state,
                    n_init=10).fit_predict(X)
    return dict(zip(valid, labels.tolist()))
