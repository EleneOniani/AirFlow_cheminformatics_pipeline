def predict_properties(smiles_list):
    """Optional ChemProp prediction. Returns {} if chemprop not installed."""
    try:
        import chemprop  # noqa: F401
    except ImportError:
        return {}
    # Plug a trained checkpoint here. Placeholder returns None per molecule.
    return {s: None for s in smiles_list}
